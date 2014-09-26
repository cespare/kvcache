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

type Shard struct {
	// Immutable configuration
	blockSize   uint64        // On-disk size limit for a block
	memCacheSize int           // Upper limit on size of in-memory cache (per-shard)
	expiry      time.Duration // How long to keep data around at all
	dir         string        // Where to keep database files

	mu *sync.Mutex // Protects all of the following

	// Block files
	seq           uint64 // Current base sequence # for wblock; seq+i+1 is the sequence # for an rblock
	wblock        *WriteBlock
	rblocks       []*ReadBlock
	rblocksCached int // wblock is always cached, so this should be <= cacheBlocks-1

	// Maps
	memCache map[string][]byte
	refCache map[string]*RecordRef
}

// NewShard creates a new Shards with the given parameters. Dir must not already exist.
func NewShard(blockSize uint64, memCacheSize int, expiry time.Duration, dir string) (*DB, error) {
	db := &DB{
		blockSize:   blockSize,
		memCacheSize: memCacheSize,
		expiry:      expiry,
		dir:         dir,

		mu:       new(sync.Mutex),
		memCache: make(map[string][]byte),
		refCache: make(map[string]*RecordRef),
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	wblock, err := NewWriteBlock(db.LogName(), db.blockSize)
	if err != nil {
		return nil, err
	}
	db.wblock = wblock
	return db, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

//func (db *DB) Debug() {
//fmt.Println()
//fmt.Printf("seq: %d\n", db.seq)
//fmt.Println("windex:")
//spew.Dump(db.windex)
//fmt.Printf("len rlogs = %d\n", len(db.rlogs))
//fmt.Println("rindices:")
//spew.Dump(db.rindices)
//fmt.Printf("rblocksCached: %d\n", db.rblocksCached)
//fmt.Println("memCache:")
//spew.Dump(db.memCache)
//fmt.Println("refCache:")
//spew.Dump(db.refCache)
//fmt.Println()
//}

func (db *DB) Get(k []byte) (v []byte, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	s := string(k)
	if v, ok := db.memCache[s]; ok {
		return v, nil
	}
	if ref, ok := db.refCache[s]; ok {
		rblock := db.rblockForSeq(ref.seq)
		r, err := rblock.ReadRecord(ref.offset)
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
	offset, err := db.wblock.WriteRecord(r)
	switch err {
	case ErrWriteLogFull:
		if err = db.Rotate(); err != nil {
			return err
		}
		offset, err = db.wblock.WriteRecord(r)
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

// Rotate removes expired blocks, reopens the write log as a read log and adds it to the list, makes a fresh
// write log, and increments the sequence number.
func (db *DB) Rotate() error {
	log.Printf("sequence %d; rotating...", db.seq)
	// Add references to refCache.
	for _, entry := range db.wblock.index {
		db.refCache[entry.key] = &RecordRef{
			seq:    db.seq,
			offset: entry.offset,
		}
	}
	// Rotate the block
	rblock, err := db.wblock.ReopenAsReadBlock()
	if err != nil {
		return err
	}
	db.rblocks = append([]*ReadBlock{rblock}, db.rblocks...)
	db.seq++
	db.rblocksCached++
	db.wblock, err = NewWriteBlock(db.LogName(), db.blockSize)
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
		for _, entry := range db.rblocks[db.rblocksCached-1].index {
			delete(db.memCache, entry.key)
		}
		db.rblocksCached--
	}
	return nil
}

func (db *DB) removeExpiredBlocks() error {
	for i := len(db.rblocks) - 1; i >= 0; i-- {
		// Remove the log file.
		rblock := db.rblocks[i]
		r, err := rblock.ReadFirstRecord()
		if err != nil {
			return err
		}
		if !db.Expired(r) {
			break
		}
		if err := rblock.Close(); err != nil {
			return err
		}
		if err := os.Remove(rblock.Filename()); err != nil {
			return err
		}
		db.rblocks = db.rblocks[:i]
		// Remove the entries in refCache and memCache as well as the index.
		cached := i < db.rblocksCached
		for _, entry := range rblock.index {
			delete(db.refCache, entry.key)
			if cached {
				delete(db.memCache, entry.key)
			}
		}
		if cached {
			db.rblocksCached--
		}
	}
	return nil
}

func (db *DB) Expired(r *Record) bool {
	return time.Since(r.t) > db.expiry
}

func (db *DB) LogName() string {
	return filepath.Join(db.dir, fmt.Sprintf("block-%6d.log", db.seq))
}

func (db *DB) rblockForSeq(seq uint64) *ReadBlock {
	return db.rblocks[int(seq-db.seq-1)]
}
