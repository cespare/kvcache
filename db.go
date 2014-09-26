package main

import (
	"errors"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
)

type DB struct {
	ldb *leveldb.DB
}

// NewDB creates a new DB at dir, which must not already exist.
func NewDB(dir string) (*DB, error) {
	ldb, err := leveldb.OpenFile(filepath.Join(dir, "kv.ldb"), nil)
	if err != nil {
		return nil, err
	}
	return &DB{ldb}, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Get(k []byte) (v []byte, err error) {
	return db.ldb.Get(k, nil)
}

func (db *DB) Put(k, v []byte) error {
	return db.ldb.Put(k, v, nil)
}
