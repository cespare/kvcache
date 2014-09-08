package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// TODO: I could shard the DB on e.g. key[0] if lock contention becomes significant.

type RecordRef struct {
	seq    uint64
	offset uint64
}

type IndexEntry struct {
	key    string // string version of the record's key
	offset uint64
}

type DB struct {
	// Immutable configuration
	blockSize   uint64        // On-disk size limit for a block
	cacheBlocks int           // Number of blocks to cache directly in memory
	expiry      time.Duration // How long to keep data around at all
	dir         string        // Where to keep database files

	mu *sync.Mutex // Protects all of the following

	// Block files
	seq           uint64 // Current base sequence number for wlog; seq+i+1 is the sequence number for an rlog
	wlog          *WriteLog
	windex        []IndexEntry // Index for wlog
	rlogs         []*ReadLog
	rindices      [][]IndexEntry
	rblocksCached int // The w block is always cached; this should be <= cacheBlocks-1

	// Maps
	memCache map[string][]byte
	refCache map[string]*RecordRef
}

// NewDB creates a new DB with the given parameters. Dir must not already exist.
func NewDB(blockSize uint64, cacheBlocks int, expiry time.Duration, dir string) (*DB, error) {
	db := &DB{
		blockSize:   blockSize,
		cacheBlocks: cacheBlocks,
		expiry:      expiry,
		dir:         dir,

		mu:       new(sync.Mutex),
		memCache: make(map[string][]byte),
		refCache: make(map[string]*RecordRef),
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	wlog, err := NewWriteLog(db.LogName(), db.blockSize)
	if err != nil {
		return nil, err
	}
	db.wlog = wlog
	return db, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Debug() {
	fmt.Println()
	fmt.Printf("seq: %d\n", db.seq)
	fmt.Println("windex:")
	spew.Dump(db.windex)
	fmt.Printf("len rlogs = %d\n", len(db.rlogs))
	fmt.Println("rindices:")
	spew.Dump(db.rindices)
	fmt.Printf("rblocksCached: %d\n", db.rblocksCached)
	fmt.Println("memCache:")
	spew.Dump(db.memCache)
	fmt.Println("refCache:")
	spew.Dump(db.refCache)
	fmt.Println()
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
	offset, err := db.wlog.WriteRecord(r)
	switch err {
	case ErrWriteLogFull:
		if err = db.Rotate(); err != nil {
			return err
		}
		offset, err = db.wlog.WriteRecord(r)
		if err != nil {
			return err
		}
	case nil:
	default:
		return err
	}
	db.windex = append(db.windex, IndexEntry{key: s, offset: offset})
	fmt.Printf("\033[01;34m>>>> v: %v\x1B[m\n", v)
	db.memCache[s] = v
	db.refCache[s] = &RecordRef{seq: db.seq, offset: offset}
	return nil
}

func (db *DB) Get(k []byte) (v []byte, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	s := string(k)
	if v, ok := db.memCache[s]; ok {
		return v, nil
	}
	if ref, ok := db.refCache[s]; ok {
		rlog := db.rlogs[int(ref.seq-db.seq-1)]
		r, err := rlog.ReadRecord(ref.offset)
		if err != nil {
			return nil, err
		}
		return r.value, nil
	}
	return nil, ErrKeyNotExist
}

// Rotate removes expired blocks, reopens the write log as a read log and adds it to the list, makes a fresh
// write log, and increments the sequence number.
func (db *DB) Rotate() error {
	log.Printf("sequence %d; rotating...", db.seq)
	defer db.Debug()
	// Add references to refCache.
	for _, entry := range db.windex {
		db.refCache[entry.key] = &RecordRef{
			seq:    db.seq,
			offset: entry.offset,
		}
	}
	// Rotate the index
	db.rindices = append([][]IndexEntry{db.windex}, db.rindices...)
	db.windex = nil
	// Rotate the log
	rlog, err := db.wlog.ReopenAsReadLog()
	if err != nil {
		return err
	}
	db.rlogs = append([]*ReadLog{rlog}, db.rlogs...)
	db.seq++
	db.rblocksCached++
	db.wlog, err = NewWriteLog(db.LogName(), db.blockSize)
	if err != nil {
		return err
	}

	if err := db.removeExpiredBlocks(); err != nil {
		return err
	}

	// If we have an extra cached block, remove it.
	if db.rblocksCached > db.cacheBlocks-1 {
		if db.rblocksCached != db.cacheBlocks {
			panic("too many blocks cached")
		}
		for _, entry := range db.rindices[db.rblocksCached-1] {
			delete(db.memCache, entry.key)
		}
	}
	return nil
}

func (db *DB) removeExpiredBlocks() error {
	for i := len(db.rlogs) - 1; i >= 0; i-- {
		// Remove the log file.
		rlog := db.rlogs[i]
		r, err := rlog.ReadFirstRecord()
		if err != nil {
			return err
		}
		if !db.Expired(r) {
			break
		}
		if err := rlog.Close(); err != nil {
			return err
		}
		if err := os.Remove(rlog.Filename()); err != nil {
			return err
		}
		db.rlogs = db.rlogs[:i]
		// Remove the entries in refCache and memCache as well as the index.
		cached := i < db.rblocksCached
		rindex := db.rindices[i]
		for _, entry := range rindex {
			delete(db.refCache, entry.key)
			if cached {
				delete(db.memCache, entry.key)
			}
		}
		db.rindices = db.rindices[:i]
	}
	return nil
}

func (db *DB) Expired(r *Record) bool {
	return time.Since(r.t) > db.expiry
}

func (db *DB) LogName() string {
	return filepath.Join(db.dir, fmt.Sprintf("block-%d.log", db.seq))
}
