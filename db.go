package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
	t   time.Time
	key []byte
	val []byte
}

type RecordRef struct {
	seq    uint64
	offset uint64
}

type DB struct {
	// Immutable configuration (once the DB is constructed)
	chunkSize   uint64        // On-disk size limit for a chunk
	cacheChunks int           // Number of chunks to cache directly in memory
	expiry      time.Duration // How long to keep data around at all
	dir         string        // Where to keep database files

	// Overridable by test functions
	now   func() time.Time              // Called once on Put, for new records
	since func(time.Time) time.Duration // Called to check expiry, on Get and Rotate

	mu *sync.Mutex // Protects all of the following

	// Chunk files
	seq           uint64 // Current base sequence # for wchunk; seq+i+1 is the sequence # for an rchunk
	wchunk        *WriteChunk
	rchunks       []*ReadChunk
	rchunksCached int // wchunk is always cached, so this should be <= cacheChunks-1

	// Maps
	memCache map[string]*Record
	refCache map[string]*RecordRef

	closed bool
}

// NewDB creates a new DB with the given parameters. Dir must not already exist.
func NewDB(chunkSize uint64, cacheChunks int, expiry time.Duration, dir string) (*DB, error) {
	db := &DB{
		chunkSize:   chunkSize,
		cacheChunks: cacheChunks,
		expiry:      expiry,
		dir:         dir,

		now:   time.Now,
		since: time.Since,

		mu:       new(sync.Mutex),
		memCache: make(map[string]*Record),
		refCache: make(map[string]*RecordRef),
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	wchunk, err := NewWriteChunk(db.wlogName(), db.chunkSize)
	if err != nil {
		return nil, err
	}
	db.wchunk = wchunk
	return db, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
	ErrDBClosed    = errors.New("database is closed")
)

func (db *DB) Get(k []byte) (v []byte, cached bool, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, false, ErrDBClosed
	}

	s := string(k)
	if r, ok := db.memCache[s]; ok {
		if db.since(r.t) > db.expiry {
			return nil, false, ErrKeyNotExist
		}
		return r.val, true, nil
	}
	if ref, ok := db.refCache[s]; ok {
		rchunk := db.rchunkForSeq(ref.seq)
		r, err := rchunk.ReadRecord(ref.offset)
		if err != nil {
			return nil, false, err
		}
		if db.since(r.t) > db.expiry {
			return nil, false, ErrKeyNotExist
		}
		return r.val, false, nil
	}
	return nil, false, ErrKeyNotExist
}

func (db *DB) Put(k, v []byte) (rotated bool, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return false, ErrDBClosed
	}

	s := string(k)

	if _, ok := db.memCache[s]; ok {
		return rotated, ErrKeyExist
	}
	if _, ok := db.refCache[s]; ok {
		return rotated, ErrKeyExist
	}

	r := &Record{
		t:   db.now(),
		key: k,
		val: v,
	}
	offset, err := db.wchunk.WriteRecord(r)
	switch err {
	case ErrWriteLogFull:
		if err = db.Rotate(); err != nil {
			return rotated, err
		}
		rotated = true
		offset, err = db.wchunk.WriteRecord(r)
		if err != nil {
			return rotated, err
		}
	case nil:
	default:
		return rotated, err
	}
	db.memCache[s] = r
	db.refCache[s] = &RecordRef{seq: db.seq, offset: offset}
	return rotated, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.closed = true
	if err := db.wchunk.Close(); err != nil {
		return err
	}
	for _, rchunk := range db.rchunks {
		if err := rchunk.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Rotate removes expired chunks, reopens the write log as a read log and adds it to the list, makes a fresh
// write log, and increments the sequence number.
func (db *DB) Rotate() error {
	log.Printf("sequence %d; rotating...", db.seq)
	// Add references to refCache.
	for _, entry := range db.wchunk.index {
		db.refCache[entry.key] = &RecordRef{
			seq:    db.seq,
			offset: entry.offset,
		}
	}
	// Rotate the chunk
	rchunk, err := db.wchunk.ReopenAsReadChunk()
	if err != nil {
		return err
	}
	db.rchunks = append([]*ReadChunk{rchunk}, db.rchunks...)
	db.seq++
	db.rchunksCached++
	db.wchunk, err = NewWriteChunk(db.wlogName(), db.chunkSize)
	if err != nil {
		return err
	}

	if err := db.removeExpiredChunks(); err != nil {
		return err
	}

	// If we have an extra cached chunk, remove it.
	if db.rchunksCached > db.cacheChunks-1 {
		if db.rchunksCached != db.cacheChunks {
			panic("too many chunks cached")
		}
		for _, entry := range db.rchunks[db.rchunksCached-1].index {
			delete(db.memCache, entry.key)
		}
		db.rchunksCached--
	}
	return nil
}

func (db *DB) removeExpiredChunks() error {
	for i := len(db.rchunks) - 1; i >= 0; i-- {
		rchunk := db.rchunks[i]
		// Check whether this whole chunk is expired by looking at the most recent timestamp.
		if db.since(rchunk.lastTimestamp) <= db.expiry {
			break
		}
		// Remove the log file.
		if err := rchunk.Close(); err != nil {
			return err
		}
		if err := os.Remove(rchunk.Filename()); err != nil {
			return err
		}
		db.rchunks = db.rchunks[:i]
		// Remove the entries in refCache and memCache as well as the index.
		cached := i < db.rchunksCached
		for _, entry := range rchunk.index {
			delete(db.refCache, entry.key)
			if cached {
				delete(db.memCache, entry.key)
			}
		}
		if cached {
			db.rchunksCached--
		}
	}
	return nil
}

func (db *DB) wlogName() string {
	return filepath.Join(db.dir, fmt.Sprintf("chunk%010d", db.seq))
}

func (db *DB) rchunkForSeq(seq uint64) *ReadChunk {
	return db.rchunks[int(db.seq-seq-1)]
}
