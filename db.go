package main

import (
	"errors"
	"sync"
)

type DB struct {
	*sync.Mutex
	m map[string][]byte
}

func NewDB() *DB {
	return &DB{
		Mutex: new(sync.Mutex),
		m:     make(map[string][]byte),
	}
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Put(k, v []byte) error {
	db.Lock()
	defer db.Unlock()

	if _, ok := db.m[string(k)]; ok {
		return ErrKeyExist
	}
	db.m[string(k)] = v
	return nil
}

func (db *DB) Get(k []byte) (v []byte, err error) {
	db.Lock()
	defer db.Unlock()

	v, ok := db.m[string(k)]
	if !ok {
		return nil, ErrKeyNotExist
	}
	return v, nil
}
