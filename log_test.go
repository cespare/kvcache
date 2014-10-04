package main

import (
	"bytes"
	"crypto/sha1"
	"strings"
	"testing"
	"time"

	"github.com/cespare/asrt"
)

const (
	testVal       = "this is the value"
	testSnappyVal = "\x11@this is the value" // snappy encoding of testVal
)

var testRecords = []*Record{
	{
		t:   ts("2014-09-21T00:00:00Z").UnixNano(),
		key: []byte("key1"),
		val: []byte(testVal),
	},
	{
		t:   ts("2014-09-21T00:00:01Z").UnixNano(),
		key: []byte("key2"),
		val: []byte(testVal),
	},
	{
		t:   ts("2014-09-21T00:00:02Z").UnixNano(),
		key: []byte("key3"),
		val: []byte(testVal),
	},
	{
		t:   ts("2014-09-21T00:00:03Z").UnixNano(),
		key: []byte("key4"),
		val: []byte(testVal),
	},
	{
		t:   ts("2014-09-21T00:00:04Z").UnixNano(),
		key: []byte("key5"),
		val: []byte(testVal),
	},
}

const (
	testLog1 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\x5e\x92\x00\x00" + "\x04key1" + "\x13" + testSnappyVal + // record 1
		"\x13\x95\xcb\x4f\x9a\x2c\xca\x00" + "\x04key2" + "\x13" + testSnappyVal // record 2
	testIdx1 = "\336idx\x00\x00\x00\x01" + // header
		"\x08key1\x08" + // record 1
		"\x08key2\x21" + // record 2
		"\x01\x4a\xf6\xb7\xa6\x83" // trailer
	testLog2 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\xd5\xc7\x94\x00" + "\x04key3" + "\x13" + testSnappyVal + // record 3
		"\x13\x95\xcb\x50\x11\x62\x5e\x00" + "\x04key4" + "\x13" + testSnappyVal // record 4
	testIdx2 = "\336idx\x00\x00\x00\x01" + // header
		"\x08key2\x08" + // record 3
		"\x08key3\x21" + // record 4
		"\x01\x4a\xd7\x29\x29\x62" // trailer
	testLog3 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x50\x4c\xfd\x28\x00" + "\x04key5" + "\x13" + testSnappyVal // record 5
	testIdx3 = "\336idx\x00\x00\x00\x01" + // header
		"\x08key5\x08" + // record 5
		"\x01\x29\x6a\xcc\x9e\x81" // trailer
)

const (
	headerLen = 8
	recordLen = 8 + 1 + 4 + 1 + len(testSnappyVal) // 33
)

func TestWriteLog(t *testing.T) {
	idxBuf := buffer{new(bytes.Buffer)}
	logBuf := buffer{new(bytes.Buffer)}
	wl, err := NewWriteLog(&idxBuf, &logBuf, 50) // Big enough for one record only.
	asrt.Equal(t, err, nil)

	// Write some vals
	offset, err := wl.WriteRecord(testRecords[0])
	asrt.Equal(t, err, nil)
	asrt.Equal(t, int(offset), headerLen)

	// We can write one more record because we don't declare wl full until we're over the max size.
	offset, err = wl.WriteRecord(testRecords[1])
	asrt.Equal(t, err, nil)
	asrt.Equal(t, int(offset), headerLen+recordLen)

	offset, err = wl.WriteRecord(testRecords[2])
	asrt.Equal(t, err, ErrWriteLogFull)

	err = wl.Close()
	asrt.Equal(t, err, nil)

	// Check the written log
	asrt.Equal(t, idxBuf.String(), testIdx1)
	asrt.Equal(t, logBuf.String(), testLog1)
}

func TestParseIndex(t *testing.T) {
	index, logSize, err := ParseIndex(strings.NewReader(testIdx1))
	asrt.Equal(t, err, nil)
	asrt.Assert(t, logSize == 74)
	asrt.DeepEqual(t, index, []IndexEntry{
		{sha("key1"), 8},
		{sha("key2"), 41},
	})
}

func TestVerifyLog(t *testing.T) {
	err := VerifyLog(strings.NewReader(testLog1), 74)
	asrt.Equal(t, err, nil)
}

func TestReadLog(t *testing.T) {
	// Log reading
	rl := OpenReadLog([]byte(testLog1))

	rec, err := rl.ReadRecord(headerLen)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:00Z").UnixNano())
	asrt.Equal(t, string(rec.key), "key1")
	asrt.Equal(t, string(rec.val), testVal)

	rec, err = rl.ReadRecord(uint32(headerLen + recordLen))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:01Z").UnixNano())
	asrt.Equal(t, string(rec.key), "key2")
	asrt.Equal(t, string(rec.val), testVal)
}

type buffer struct {
	*bytes.Buffer
}

func (b *buffer) Close() error { return nil }

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func sha(s string) keyHash {
	return sha1.Sum([]byte(s))
}
