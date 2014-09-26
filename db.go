package main

import (
	"crypto/sha1"
	"fmt"
	"path/filepath"
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
	// All state is read-only after DB has been initially created.
	numShards      int
	partitionBytes int // The number of bytes of the key hash to use for partitioning purposes
	shards         []*Shard
}

func NewDB(numShards uint, blockSize, memCacheSize uint64, expiry time.Duration, dir string) (*DB, error) {
	if numShards == 0 {
		return nil, fmt.Errorf("%d is an invalid number of shards (must be positive)", numShards)
	}
	if numShards > 2e10 {
		return nil, fmt.Errorf("%d seems like an unreasonably large number of shards", numShards)
	}
	if !isPow2(numShards) {
		return nil, fmt.Errorf("%d is an invalid number of shards (must be a power of 2)", numShards)
	}

	shards := make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardDir := filepath.Join(dir, fmt.Sprintf("shard%4d", i))
		var err error
		shards[i], err = NewShard(blockSize, memCacheSize/numShards, expiry, shardDir)
		if err != nil {
			return nil, err
		}
	}

	return &DB{
		numShards:      numShards,
		partitionBytes: (log2(numShards) + 7) / 8, // The number of bytes required to represent at least numShards
		shards:         shards,
	}, nil
}

func (db *DB) Get(k []byte) (v []byte, err error) { return db.chooseShard(k).Get(k) }
func (db *DB) Put(k, v []byte) error              { return db.chooseShard(k).Put(k, v) }

func (db *DB) chooseShard(key []byte) *Shard {
	hash := sha1.Sum(key)
	var n uint
	for i := 0; i < db.partitionBytes; i++ {
		n = n<<8 + uint(hash[i])
	}
	return db.shards[n%db.numShards]
}

// isPow2 returns whether n is a power of 2.
func isPow2(n uint) bool {
	if n == 0 {
		return true
	}
	// https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetKernighan
	var c int
	for c = 0; v > 0; c++ {
		v &= v - 1
	}
	return c == 1
}

func log2(n uint) uint {
	// https://graphics.stanford.edu/~seander/bithacks.html#IntegerLogObvious
	var r uint
	for n >>= 1; n > 0; n >>= 1 {
		r++
	}
	return r
}
