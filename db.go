package main

import (
	"errors"
	"os"
	"path/filepath"
)

type DB struct {
	f *os.File
}

// NewDB creates a new DB with the given parameters. Dir must not already exist.
func NewDB(dir string) (*DB, error) {
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "f.db"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &DB{f}, nil
}

var (
	ErrKeyNotExist = errors.New("key does not exist in the database")
	ErrKeyExist    = errors.New("key already exists in the database")
)

func (db *DB) Get(k []byte) (v []byte, err error) {
	panic("unimplemented")
}

func (db *DB) Put(k, v []byte) error {
	if _, err := db.f.Write(k); err != nil {
		return err
	}
	if _, err := db.f.Write(v); err != nil {
		return err
	}
	return nil
}
