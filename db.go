package main

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cespare/kvcache/b"
)

type DB struct {
	*sync.Mutex
	btree *b.Tree
}

func NewDB() *DB {
	return &DB{
		Mutex: new(sync.Mutex),
		btree: b.TreeNew(bytes.Compare),
	}
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Put(k, v []byte) error {
	db.Lock()
	defer db.Unlock()

	_, written := db.btree.Put(k, func(_ []byte, exists bool) (newValue []byte, write bool) {
		if exists {
			return nil, false
		}
		return v, true
	})
	if !written {
		return ErrKeyExist
	}
	return nil
}

func (db *DB) Get(k []byte) (v []byte, err error) {
	db.Lock()
	defer db.Unlock()

	v, ok := db.btree.Get(k)
	if !ok {
		return nil, ErrKeyNotExist
	}
	return v, nil
}
