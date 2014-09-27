package main

// Structures:
// 256-shard map pair (cache, ref), each with a lock
// write lock that must be held when accessing/modifying the write chunk
// read lock that must be held when accessing/modfiying the read chunks

// Put:
// - Lock map shard
// - Look up value in cache; if it exists return error and unlock
// - Look up value in ref map; if it exists return error and unlock
// - Serialize kv
// - Lock write lock
// - Write into write chunk and get offset
// - Unlock write lock
// - Write into ref map
// - Write into cache
// - Unlock map shard
//
// Get:
// - Lock map shard
// - Look up value in cache; if it exists return it and unlock
// - Look up value in ref map; if it doesn't exist return error and unlock
// - Lock the read lock
// - Read the value and return it
// - Unlock the read lock
// - Unlock the map shard
//
// Rotate:
// - Lock wlock
// - Lock rlock
// - NOTE: do not lock any of the shard locks inside here -- that makes deadlocks
// - Get the full list of refs and cache entries to delete
// - Rotate the out-of-date read chunks
// - Reopen the write chunk as the first read chunk
// - Unlock rlock
// - Unlock wlock
// - For each shard, remove all expired ref and cache entries

type DB struct {
}

func NewDB() (*DB, error) {
	panic("unimplemented")
}


var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Get(k []byte) (v []byte, err error) {
	panic("unimplemented")
}

func (db *DB) Put(k, v []byte) error {
	panic("unimplemented")
}
