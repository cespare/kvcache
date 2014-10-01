package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cespare/asrt"
)

func TestDB(t *testing.T) {
	tempdir, err := ioutil.TempDir(".", "kvcache-test-")
	asrt.Equal(t, err, nil)
	defer os.RemoveAll(tempdir)
	dir := filepath.Join(tempdir, "db")

	var now time.Time

	// See log_test for info about the test data

	db, err := NewDB(50, 2, 10*time.Second, dir)
	asrt.Equal(t, err, nil)
	db.now = func() time.Time { return now }
	db.since = func(t time.Time) time.Duration { return now.Sub(t) }

	// Put/Get some values in the first chunk. Values come out of cache.
	now = ts("2014-09-21T00:00:00Z")
	rotated, err := db.Put(testRecords[0].key, testRecords[0].val)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	v, cached, err := db.Get(testRecords[0].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, testRecords[0].val))

	now = ts("2014-09-21T00:00:01Z")
	rotated, err = db.Put(testRecords[1].key, testRecords[1].val)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	v, cached, err = db.Get(testRecords[1].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, testRecords[0].val))

	for _, record := range testRecords[:2] {
		_, err = db.Put(record.key, []byte("asdf"))
		asrt.Equal(t, err, ErrKeyExist)
	}

	asrt.DeepEqual(t, lsDir(dir), []string{"chunk0000000000.idx", "chunk0000000000.log"})

	// Rotate
	now = ts("2014-09-21T00:00:02Z")
	rotated, err = db.Put(testRecords[2].key, testRecords[3].val)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, true)

	v, cached, err = db.Get(testRecords[0].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, testRecords[0].val))

	v, cached, err = db.Get(testRecords[2].key)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, cached, true)
	asrt.Assert(t, bytes.Equal(v, testRecords[2].val))

	asrt.DeepEqual(t, lsDir(dir),
		[]string{"chunk0000000000.idx", "chunk0000000000.log", "chunk0000000001.idx", "chunk0000000001.log"})

	now = ts("2014-09-21T00:00:03Z")
	rotated, err = db.Put(testRecords[3].key, testRecords[3].val)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, false)

	// Rotate again
	now = ts("2014-09-21T00:00:04Z")
	rotated, err = db.Put(testRecords[4].key, testRecords[4].val)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rotated, true)

	// Third block is no longer cached
	for _, record := range testRecords[:2] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
		asrt.Equal(t, cached, false)
		asrt.Assert(t, bytes.Equal(v, record.val))
	}

	for _, record := range testRecords[2:] {
		v, cached, err = db.Get(record.key)
		asrt.Equal(t, err, nil)
		asrt.Equal(t, cached, true)
		asrt.Assert(t, bytes.Equal(v, record.val))
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

	// TODO: test:
	// - reopening the DB
	// - expired cached values
	// - expired non-cached values
	// - deletion of expired blocks
}

// lsDir returns a sorted list of filenames in dir.
func lsDir(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, len(files))
	for i, fi := range files {
		names[i] = fi.Name()
	}
	return names
}
