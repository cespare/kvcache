package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cespare/kvcache/internal/github.com/cespare/asrt"
)

func init() { log.SetOutput(ioutil.Discard) }

func TestDB(t *testing.T) {
	tempdir, err := ioutil.TempDir(".", "testdata-")
	asrt.Equal(t, err, nil)
	defer os.RemoveAll(tempdir)
	dir := filepath.Join(tempdir, "db")

	var now time.Time

	// See log_test for info about the test data

	db, err := NewDB(50, 10*time.Second, dir)
	asrt.Equal(t, err, nil)
	db.now = func() time.Time { return now }
	db.since = func(t time.Time) time.Duration { return now.Sub(t) }

	// Put/Get some values in the first chunk. Values come out of cache.
	now = ts("2014-09-21T00:00:00Z")
	rotated, err := db.Put(testRecords[0].key, []byte(testVal))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	v, cached, err := db.Get(testRecords[0].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, []byte(testVal)))

	now = ts("2014-09-21T00:00:01Z")
	rotated, err = db.Put(testRecords[1].key, []byte(testVal))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	v, cached, err = db.Get(testRecords[1].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, []byte(testVal)))

	for _, record := range testRecords[:2] {
		_, err = db.Put(record.key, []byte("asdf"))
		asrt.Equal(t, err, ErrKeyExist)
	}

	asrt.DeepEqual(t, lsDir(dir), []string{"chunk0000000000.idx", "chunk0000000000.log"})

	// Rotate
	now = ts("2014-09-21T00:00:02Z")
	rotated, err = db.Put(testRecords[2].key, []byte(testVal))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, true)

	v, cached, err = db.Get(testRecords[0].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, false)
	asrt.Assert(t, bytes.Equal(v, []byte(testVal)))

	v, cached, err = db.Get(testRecords[2].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, []byte(testVal)))

	asrt.DeepEqual(t, lsDir(dir),
		[]string{"chunk0000000000.idx", "chunk0000000000.log", "chunk0000000001.idx", "chunk0000000001.log"})

	now = ts("2014-09-21T00:00:03Z")
	rotated, err = db.Put(testRecords[3].key, []byte(testVal))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	// Rotate again
	now = ts("2014-09-21T00:00:04Z")
	rotated, err = db.Put(testRecords[4].key, []byte(testVal))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, true)

	for i, record := range testRecords {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
		asrt.Equal(t, cached, i == 4)
		asrt.Assert(t, bytes.Equal(v, []byte(testVal)))
	}

	// Close and check the data
	err = db.Close()
	asrt.Equal(t, err, nil)

	files := []string{"chunk0000000000.idx", "chunk0000000000.log", "chunk0000000001.idx",
		"chunk0000000001.log", "chunk0000000002.idx", "chunk0000000002.log"}
	asrt.DeepEqual(t, lsDir(dir), files)
	for i, want := range []string{testLog1, testLog2, testLog3} {
		got, err := ioutil.ReadFile(filepath.Join(dir, files[i*2+1]))
		asrt.Equal(t, err, nil)
		asrt.Equal(t, string(got), want)
	}

	// Reopen the DB
	db, _, err = OpenDB(50, 10*time.Second, dir, false)
	asrt.Equal(t, err, nil)
	db.now = func() time.Time { return now }
	db.since = func(t time.Time) time.Duration { return now.Sub(t) }

	for _, record := range testRecords {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
		asrt.Equal(t, cached, false)
		asrt.Assert(t, bytes.Equal(v, []byte(testVal)))
	}

	// "Expire" everything older than 2014-09-21T00:00:03Z
	now = ts("2014-09-21T00:00:13Z")

	for _, record := range testRecords[:3] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, ErrKeyNotExist)
	}

	for _, record := range testRecords[3:] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
	}

	// Rotate and ensure the same key/vals are visible, and that the
	// fully expired block was deleted.
	err = db.Rotate()
	asrt.Equal(t, err, nil)

	for _, record := range testRecords[:3] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, ErrKeyNotExist)
	}

	for _, record := range testRecords[3:] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
	}

	// Two new (empty) chunks were added: one was the new write chunk created on OpenDB;
	// the other was made on rotation.
	files = []string{"chunk0000000001.idx", "chunk0000000001.log", "chunk0000000002.idx", "chunk0000000002.log",
		"chunk0000000003.idx", "chunk0000000003.log", "chunk0000000004.idx", "chunk0000000004.log"}
	asrt.DeepEqual(t, lsDir(dir), files)

	err = db.Close()
	asrt.Equal(t, err, nil)
	asrt.DeepEqual(t, lsDir(dir), files)

	// Intentionally corrupt index 3.
	err = os.Truncate(filepath.Join(dir, "chunk0000000003.idx"), 50)
	asrt.Equal(t, err, nil)
	db, removedChunks, err := OpenDB(50, 10*time.Second, dir, true)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, removedChunks, int64(1))
	files = []string{"chunk0000000001.idx", "chunk0000000001.log", "chunk0000000002.idx", "chunk0000000002.log",
		"chunk0000000004.idx", "chunk0000000004.log", "chunk0000000005.idx", "chunk0000000005.log"}
	asrt.DeepEqual(t, lsDir(dir), files)
	asrt.Equal(t, err, nil)
	err = db.Close()
	asrt.Equal(t, err, nil)
}
