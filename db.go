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
	t     time.Time
	key   []byte
	value []byte
}

type RecordRef struct {
	seq    uint64
	offset uint64
}

type DB struct {
	// Immutable configuration
	chunkSize   uint64        // On-disk size limit for a chunk
	cacheChunks int           // Number of chunks to cache directly in memory
	expiry      time.Duration // How long to keep data around at all
	dir         string        // Where to keep database files

	mu *sync.Mutex // Protects all of the following

	// Chunk files
	seq           uint64 // Current base sequence # for wchunk; seq+i+1 is the sequence # for an rchunk
	wchunk        *WriteChunk
	rchunks       []*ReadChunk
	rchunksCached int // wchunk is always cached, so this should be <= cacheChunks-1

	// Maps
	memCache map[string][]byte
	refCache map[string]*RecordRef
}

// NewDB creates a new DB with the given parameters. Dir must not already exist.
func NewDB(chunkSize uint64, cacheChunks int, expiry time.Duration, dir string) (*DB, error) {
	db := &DB{
		chunkSize:   chunkSize,
		cacheChunks: cacheChunks,
		expiry:      expiry,
		dir:         dir,

		mu:       new(sync.Mutex),
		memCache: make(map[string][]byte),
		refCache: make(map[string]*RecordRef),
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	wchunk, err := NewWriteChunk(db.LogName(), db.chunkSize)
	if err != nil {
		return nil, err
	}
	db.wchunk = wchunk
	return db, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Get(k []byte) (v []byte, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	s := string(k)
	if v, ok := db.memCache[s]; ok {
		return v, nil
	}
	if ref, ok := db.refCache[s]; ok {
		rchunk := db.rchunkForSeq(ref.seq)
		r, err := rchunk.ReadRecord(ref.offset)
		if err != nil {
			return nil, err
		}
		return r.value, nil
	}
	return nil, ErrKeyNotExist
}

func (db *DB) Put(k, v []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	s := string(k)

	if _, ok := db.memCache[s]; ok {
		return ErrKeyExist
	}
	if _, ok := db.refCache[s]; ok {
		return ErrKeyExist
	}

	r := &Record{
		t:     time.Now(),
		key:   k,
		value: v,
	}
	offset, err := db.wchunk.WriteRecord(r)
	switch err {
	case ErrWriteLogFull:
		if err = db.Rotate(); err != nil {
			return err
		}
		offset, err = db.wchunk.WriteRecord(r)
		if err != nil {
			return err
		}
	case nil:
	default:
		return err
	}
	db.memCache[s] = v
	db.refCache[s] = &RecordRef{seq: db.seq, offset: offset}
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
	db.wchunk, err = NewWriteChunk(db.LogName(), db.chunkSize)
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
		if time.Since(rchunk.lastTimestamp) <= db.expiry {
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

func (db *DB) LogName() string {
	return filepath.Join(db.dir, fmt.Sprintf("chunk%10d.log", db.seq))
}

func (db *DB) rchunkForSeq(seq uint64) *ReadChunk {
	return db.rchunks[int(seq-db.seq-1)]
}
