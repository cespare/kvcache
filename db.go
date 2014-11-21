package main

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/snappy"
)

type Record struct {
	// NOTE: using a time in unix nanoseconds to avoid using a larger time.Time that contains a pointer.
	t   int64
	key []byte
	val []byte
}

type EncodedRecord struct {
	t         int64
	hash      keyHash
	key       []byte
	snappyVal []byte
}

type RecordRef struct {
	seq    uint32
	offset uint32
}

type keyHash [20]byte // The SHA-1 hash of a key

type DB struct {
	// Immutable configuration (once the DB is constructed)
	chunkSize uint64        // On-disk size limit for a chunk
	expiry    time.Duration // How long to keep data around at all
	dir       string        // Where to keep database files

	// Overridable by test functions
	now   func() time.Time              // Called once on Put, for new records
	since func(time.Time) time.Duration // Called to check expiry, on Get and Rotate

	mu *sync.Mutex // Protects all of the following

	// Chunk files
	seq     uint32 // Current base sequence # for wchunk; seq+i+1 is the sequence # for an rchunk
	wchunk  *WriteChunk
	rchunks []*ReadChunk

	memCache map[keyHash]Record // entries in wchunk are cached directly
	refCache *RefMap

	closed  bool
	dirFile *os.File // Handle for flocking the DB
}

func newDB(chunkSize uint64, expiry time.Duration, dir string) (*DB, error) {
	if chunkSize > math.MaxUint32 {
		return nil, fmt.Errorf("%d is too large for a chunk size (cannot be larger than 1<<32)")
	}
	return &DB{
		chunkSize: chunkSize,
		expiry:    expiry,
		dir:       dir,

		now:   time.Now,
		since: time.Since,

		mu:       new(sync.Mutex),
		memCache: make(map[keyHash]Record),
		refCache: NewRefMap(),
	}, nil
}

var ErrDBDirExists = errors.New("DB dir already exists")

// NewDB creates a new DB with the given parameters. Dir must not already exist.
func NewDB(chunkSize uint64, expiry time.Duration, dir string) (*DB, error) {
	if err := os.Mkdir(dir, 0700); err != nil {
		if os.IsExist(err) {
			return nil, ErrDBDirExists
		}
		return nil, err
	}
	db, err := newDB(chunkSize, expiry, dir)
	if err != nil {
		return nil, err
	}
	if err := db.addFlock(); err != nil {
		return nil, err
	}
	wchunk, err := NewWriteChunk(db.logName(db.seq), db.chunkSize)
	if err != nil {
		return nil, err
	}
	db.wchunk = wchunk
	return db, nil
}

// OpenDB opens an existing DB, or else creates a new DB if dir does not exist.
func OpenDB(chunkSize uint64, expiry time.Duration, dir string) (*DB, error) {
	db, err := NewDB(chunkSize, expiry, dir)
	switch err {
	case ErrDBDirExists:
	case nil:
		return db, err
	default:
		return nil, err
	}
	db, err = newDB(chunkSize, expiry, dir)
	if err != nil {
		return nil, err
	}
	if err := db.addFlock(); err != nil {
		return nil, err
	}
	if err := db.loadReadChunks(dir); err != nil {
		return nil, err
	}

	wchunk, err := NewWriteChunk(db.logName(db.seq), db.chunkSize)
	if err != nil {
		return nil, err
	}
	db.wchunk = wchunk
	return db, nil
}

type DBStats struct {
	RChunks       int
	TotalRLogSize uint64
	WLogKeys      int
	RLogKeys      int
	TotalKeys     int
}

func (s DBStats) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Read chunks: %d\n", s.RChunks)
	fmt.Fprintf(&buf, "Total read log size: %d\n", s.TotalRLogSize)
	fmt.Fprintf(&buf, "Keys in write log: %d\n", s.WLogKeys)
	fmt.Fprintf(&buf, "Keys in read log: %d\n", s.RLogKeys)
	fmt.Fprintf(&buf, "Total keys: %d\n", s.TotalKeys)
	return buf.String()
}

func (db *DB) Info() DBStats {
	db.mu.Lock()
	defer db.mu.Unlock()

	var totalSize uint64
	for _, rchunk := range db.rchunks {
		totalSize += uint64(len(rchunk.b))
	}
	return DBStats{
		RChunks:       len(db.rchunks),
		TotalRLogSize: totalSize,
		WLogKeys:      len(db.memCache),
		RLogKeys:      db.refCache.Len(),
		TotalKeys:     len(db.memCache) + db.refCache.Len(),
	}
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
	ErrDBClosed    = errors.New("database is closed")
)

func (db *DB) Get(k []byte) (v []byte, cached bool, err error) {
	hash := sha1.Sum(k)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, false, ErrDBClosed
	}

	if r, ok := db.memCache[hash]; ok {
		if db.since(time.Unix(0, r.t)) > db.expiry {
			return nil, false, ErrKeyNotExist
		}
		return r.val, true, nil
	}
	if ref, ok := db.refCache.Get(hash); ok {
		rchunk := db.rchunkForSeq(ref.seq)
		r, err := rchunk.ReadRecord(ref.offset)
		if err != nil {
			return nil, false, err
		}
		if db.since(time.Unix(0, r.t)) > db.expiry {
			return nil, false, ErrKeyNotExist
		}
		return r.val, false, nil
	}
	return nil, false, ErrKeyNotExist
}

type FatalDBError struct {
	error
}

func (e FatalDBError) Error() string { return e.error.Error() }

var ErrValTooLong = errors.New("value is too long")

func (db *DB) Put(k, v []byte) (rotated bool, err error) {
	hash := sha1.Sum(k)
	if len(v) > maxValLen {
		return false, ErrValTooLong
	}
	snappyVal := SnappyEncode(v)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrDBClosed
	}

	if _, ok := db.memCache[hash]; ok {
		return rotated, ErrKeyExist
	}
	if _, ok := db.refCache.Get(hash); ok {
		return rotated, ErrKeyExist
	}

	t := db.now().UnixNano()
	r := Record{
		t:   t,
		key: k,
		val: v,
	}
	er := EncodedRecord{
		t:         t,
		hash:      hash,
		key:       k,
		snappyVal: snappyVal,
	}
	offset, err := db.wchunk.WriteRecord(&er)
	switch err {
	case ErrWriteLogFull:
		if err = db.Rotate(); err != nil {
			if err2 := db.close(); err2 != nil {
				log.Println("Error while closing DB:", err2)
			}
			return rotated, FatalDBError{err}
		}
		rotated = true
		offset, err = db.wchunk.WriteRecord(&er)
		if err != nil {
			return rotated, err
		}
	case nil:
	default:
		return rotated, err
	}
	db.memCache[hash] = r
	db.refCache.Put(hash, RecordRef{seq: db.seq, offset: offset})
	return rotated, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.close()
}

func (db *DB) close() error {
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.wchunk.Close(); err != nil {
		return err
	}
	for _, rchunk := range db.rchunks {
		if err := rchunk.Close(); err != nil {
			return err
		}
	}
	return db.removeFlock()
}

// Rotate removes expired chunks, reopens the write log as a read log and adds it to the list, makes a fresh
// write log, and increments the sequence number.
func (db *DB) Rotate() error {
	log.Printf("sequence %d; rotating...", db.seq)
	// Add references to refCache.
	for _, entry := range db.wchunk.index {
		db.refCache.Put(entry.hash, RecordRef{
			seq:    db.seq,
			offset: entry.offset,
		})
	}
	// Open the new chunk -- do this early, because this is where errors typically occur
	if db.seq == math.MaxUint32 {
		panic("sequence number wrapped")
	}
	db.seq++
	wchunk, err := NewWriteChunk(db.logName(db.seq), db.chunkSize)
	if err != nil {
		return err
	}

	// Rotate the chunk
	rchunk, err := db.wchunk.ReopenAsReadChunk()
	if err != nil {
		return err
	}
	db.rchunks = append([]*ReadChunk{rchunk}, db.rchunks...)
	db.wchunk = wchunk

	if err := db.removeExpiredChunks(); err != nil {
		return err
	}

	// Clear the memCache. Size estimate based on previous cache.
	db.memCache = make(map[keyHash]Record, len(db.memCache))

	return nil
}

func (db *DB) removeExpiredChunks() error {
	for i := len(db.rchunks) - 1; i >= 0; i-- {
		rchunk := db.rchunks[i]
		// Check whether this whole chunk is expired by looking at the most recent timestamp.
		if db.since(time.Unix(0, rchunk.lastTimestamp)) <= db.expiry {
			break
		}
		// Remove the log file.
		if err := rchunk.Close(); err != nil {
			return err
		}
		for _, filename := range rchunk.Filenames() {
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
		db.rchunks = db.rchunks[:i]
		// Remove the refCache entries
		for _, entry := range rchunk.index {
			db.refCache.Delete(entry.hash)
		}
	}
	return nil
}

const chunkFormat = "chunk%010d"

func (db *DB) logName(seq uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf(chunkFormat, seq))
}

func (db *DB) rchunkForSeq(seq uint32) *ReadChunk {
	return db.rchunks[int(db.seq-seq-1)]
}

func (db *DB) addFlock() error {
	f, err := os.Open(db.dir)
	if err != nil {
		return err
	}
	db.dirFile = f
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("cannot lock database dir; is it currently in use? (err = %v)", err)
	}
	return nil
}

func (db *DB) removeFlock() error {
	defer db.dirFile.Close()
	return syscall.Flock(int(db.dirFile.Fd()), syscall.LOCK_UN)
}

func (db *DB) loadReadChunks(dir string) error {
	start := time.Now()
	seqs, err := findDBFiles(dir)
	if err != nil {
		return err
	}
	if len(seqs) > 0 {
		db.seq = seqs[len(seqs)-1] + 1
	}
	log.Printf("Found %d existing chunks; next seq=%d", len(seqs), db.seq)

	type task struct {
		i      int
		seq    uint32
		index  []IndexEntry
		rchunk *ReadChunk
	}

	// Parse and load the read chunks in parallel -- it's fairly CPU-intensive.
	// TODO: this fan-out fan-in logic can definitely be simplified.
	inputc := make(chan *task)
	outputc := make(chan *task)
	errc := make(chan error)
	var wg sync.WaitGroup
	go func() {
		for i, seq := range seqs {
			inputc <- &task{
				i:   i,
				seq: seq,
			}
		}
		close(inputc)
	}()
	const procs = 32
	wg.Add(procs)
	for i := 0; i < procs; i++ {
		go func() {
			defer wg.Done()
			for task := range inputc {
				index, rchunk, err := LoadReadChunk(db.logName(task.seq))
				if err != nil {
					errc <- err
					return
				}
				task.index = index
				task.rchunk = rchunk
				outputc <- task
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	db.rchunks = make([]*ReadChunk, len(seqs))
aggregate:
	for {
		select {
		case err := <-errc:
			go func() {
				// Drain chans to free up goroutines
				for _ = range inputc {
				}
				for _ = range outputc {
				}
				for _ = range errc {
				}
			}()
			return err
		case task := <-outputc:
			for _, entry := range task.index {
				db.refCache.Put(entry.hash, RecordRef{
					seq:    task.seq,
					offset: entry.offset,
				})
			}
			// The chunks are stored in reverse order, with the highest-numbered (most recent) chunk at the front.
			db.rchunks[len(seqs)-task.i-1] = task.rchunk
			log.Printf("Loaded chunk %d", task.seq)
		case <-done:
			break aggregate
		}
	}

	log.Printf("Finished loading DB; loaded %d chunks in %.3fs",
		len(seqs), time.Since(start).Seconds())
	return nil
}

var (
	ErrDBFilesMismatch = errors.New("DB index/log files do not match")
	ErrBadIdxFilename  = errors.New("DB index file (.idx) has an invalid name.")
	ErrBadLogFilename  = errors.New("DB log file (.log) has an invalid name.")
)

// findDBFiles discovers and sanity-checks the DB files in dir.
// Index and log files must be paired. There cannot be holes in the sequence.
// Sequence numbers are returned in sorted order.
func findDBFiles(dir string) (seqs []uint32, err error) {
	var idxFiles []string
	var logFiles []string
	for _, filename := range lsDir(dir) {
		f := filepath.Base(filename)
		if strings.HasSuffix(f, ".idx") {
			idxFiles = append(idxFiles, f)
		}
		if strings.HasSuffix(f, ".log") {
			logFiles = append(logFiles, f)
		}
	}
	if len(idxFiles) != len(logFiles) {
		return nil, ErrDBFilesMismatch
	}
	for i, idx := range idxFiles {
		basename := strings.TrimSuffix(idx, ".idx")
		seq, err := seqFromBasename(basename)
		if err != nil {
			return nil, ErrBadIdxFilename
		}
		if i > 0 {
			if seq != seqs[i-1]+1 {
				return nil, fmt.Errorf("Invalid DB files: skip from seq %d to %d", seqs[i-1], seq)
			}
		}
		if logFiles[i] != basename+".log" {
			return nil, ErrDBFilesMismatch
		}
		seqs = append(seqs, seq)
	}
	return seqs, nil
}

// lsDir returns a sorted list of filenames in dir.
func lsDir(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, len(files))
	for i, fi := range files {
		names[i] = fi.Name()
	}
	return names
}

// seqFromBasename turns a base chunk name (like "chunk0000000123") into a sequence number (like 123).
func seqFromBasename(basename string) (seq uint32, err error) {
	_, err = fmt.Sscanf(basename, chunkFormat, &seq)
	return
}

func SnappyEncode(b []byte) []byte {
	enc, err := snappy.Encode(nil, b)
	if err != nil {
		// snappy.Encode never produces a non-nil error.
		// https://code.google.com/p/snappy-go/issues/detail?id=8
		panic("cannot happen")
	}
	return enc
}
