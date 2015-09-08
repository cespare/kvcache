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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/kvcache/internal/github.com/cespare/snappy"
	"github.com/cespare/kvcache/internal/github.com/cespare/wait"
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
	seq     uint32 // Current base sequence # for wchunk
	wchunk  *WriteChunk
	rchunks map[uint32]*ReadChunk

	memCache map[keyHash]Record // entries in wchunk are cached directly
	refCache map[keyHash]RecordRef

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
		rchunks:  make(map[uint32]*ReadChunk),
		memCache: make(map[keyHash]Record),
		refCache: make(map[keyHash]RecordRef),
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
func OpenDB(chunkSize uint64, expiry time.Duration, dir string, removeCorrupt bool) (db *DB, removedChunks int64, err error) {
	db, err = NewDB(chunkSize, expiry, dir)
	switch err {
	case ErrDBDirExists:
	case nil:
		return db, removedChunks, err
	default:
		return nil, removedChunks, err
	}
	db, err = newDB(chunkSize, expiry, dir)
	if err != nil {
		return nil, removedChunks, err
	}
	if err := db.addFlock(); err != nil {
		return nil, removedChunks, err
	}
	removedChunks, err = db.loadReadChunks(dir, removeCorrupt)
	if err != nil {
		return nil, removedChunks, err
	}

	wchunk, err := NewWriteChunk(db.logName(db.seq), db.chunkSize)
	if err != nil {
		return nil, removedChunks, err
	}
	db.wchunk = wchunk
	return db, removedChunks, nil
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
		RLogKeys:      len(db.refCache),
		TotalKeys:     len(db.memCache) + len(db.refCache),
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
	if ref, ok := db.refCache[hash]; ok {
		rchunk := db.rchunks[ref.seq]
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
	if len(v) > maxValLen {
		return false, ErrValTooLong
	}

	// compute as much as we can outside the lock
	hash := sha1.Sum(k)
	snappyVal := SnappyEncode(v)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrDBClosed
	}

	if _, ok := db.memCache[hash]; ok {
		return rotated, ErrKeyExist
	}
	if _, ok := db.refCache[hash]; ok {
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
	db.refCache[hash] = RecordRef{seq: db.seq, offset: offset}
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
		db.refCache[entry.hash] = RecordRef{
			seq:    db.seq,
			offset: entry.offset,
		}
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
	db.rchunks[db.seq-1] = rchunk
	db.wchunk = wchunk

	if err := db.removeExpiredChunks(); err != nil {
		return err
	}

	// Clear the memCache. Size estimate based on previous cache.
	db.memCache = make(map[keyHash]Record, len(db.memCache))

	return nil
}

func (db *DB) removeExpiredChunks() error {
	var seqs []uint32
	for seq := range db.rchunks {
		seqs = append(seqs, seq)
	}
	sort.Sort(uint32s(seqs))

	for _, seq := range seqs {
		rchunk := db.rchunks[seq]
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
		delete(db.rchunks, seq)
		// Remove the refCache entries
		for _, entry := range rchunk.index {
			delete(db.refCache, entry.hash)
		}
	}
	return nil
}

const chunkFormat = "chunk%010d"

func (db *DB) logName(seq uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf(chunkFormat, seq))
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

// loadReadChunks loads DB chunks (pairs of .log/.idx files) from dir.
// A non-nil error is returned if the files are mismatched.
// If there is a corrupt chunk, then it is removed or an error is returned,
// depending on removeCorrupt.
func (db *DB) loadReadChunks(dir string, removeCorrupt bool) (removedChunks int64, err error) {
	start := time.Now()
	seqs, err := findDBFiles(dir)
	if err != nil {
		return removedChunks, err
	}
	log.Printf("Found %d existing chunks", len(seqs))

	// Parse and load the read chunks in parallel -- it's fairly CPU-intensive.
	inputc := make(chan *loadChunkTask)
	outputc := make(chan *loadChunkTask)
	var wg wait.Group
	wg.Go(func(quit <-chan struct{}) error {
		defer close(inputc)
		for _, seq := range seqs {
			select {
			case inputc <- &loadChunkTask{seq: seq}:
			case <-quit:
				return nil
			}
		}
		return nil
	})
	// Spin up a bunch of workers to load the chunks.
	for i := 0; i < 32; i++ {
		wg.Go(db.makeLoadChunkWorker(outputc, inputc, removeCorrupt, &removedChunks))
	}
	done := make(chan error)
	go func() {
		done <- wg.Wait()
	}()
aggregate:
	for {
		select {
		case err := <-done:
			if err != nil {
				return removedChunks, err
			}
			break aggregate
		case task := <-outputc:
			if task.seq >= db.seq {
				db.seq = task.seq + 1
			}
			for _, entry := range task.index {
				db.refCache[entry.hash] = RecordRef{
					seq:    task.seq,
					offset: entry.offset,
				}
			}
			db.rchunks[task.seq] = task.rchunk
			log.Printf("Loaded chunk %d", task.seq)
		}
	}
	log.Printf("Next sequence number: %d", db.seq)
	log.Printf("Finished loading DB; loaded %d chunks in %.3fs", len(seqs), time.Since(start).Seconds())
	return removedChunks, nil
}

type loadChunkTask struct {
	seq    uint32
	index  []IndexEntry
	rchunk *ReadChunk
}

func (db *DB) makeLoadChunkWorker(outputc, inputc chan *loadChunkTask, removeCorrupt bool, removedChunks *int64) func(<-chan struct{}) error {
	return func(quit <-chan struct{}) error {
		for {
			select {
			case task, ok := <-inputc:
				if !ok {
					return nil
				}
				logName := db.logName(task.seq)
				index, rchunk, err := LoadReadChunk(logName)
				if err != nil {
					log.Printf("Found corrupt chunk %d", task.seq)
					if !removeCorrupt {
						return err
					}
					atomic.AddInt64(removedChunks, 1)
					for _, filename := range []string{logName + ".idx", logName + ".log"} {
						log.Printf("Removing file %s from corrupt chunk %d", filename, task.seq)
						if err := os.Remove(filename); err != nil {
							return err
						}
					}
					break
				}
				task.index = index
				task.rchunk = rchunk
				select {
				case outputc <- task:
				case <-quit:
					return nil
				}
			case <-quit:
				return nil
			}
		}
	}
}

var (
	ErrDBFilesMismatch = errors.New("DB index/log files do not match")
	ErrBadIdxFilename  = errors.New("DB index file (.idx) has an invalid name")
	ErrBadLogFilename  = errors.New("DB log file (.log) has an invalid name")
)

// findDBFiles discovers the DB files in dir. Index and log files must be paired.
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

type uint32s []uint32

func (s uint32s) Len() int           { return len(s) }
func (s uint32s) Less(i, j int) bool { return s[i] < s[j] }
func (s uint32s) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
