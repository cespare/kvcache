package main

import (
	"bytes"
	"crypto/sha1"
	"strings"
	"testing"
	"time"

	"github.com/cespare/kvcache/internal/github.com/cespare/asrt"
)

const (
	testVal       = "this is the value"
	testSnappyVal = "\x11@this is the value" // snappy encoding of testVal
)

var testRecords = []*EncodedRecord{
	{
		t:         ts("2014-09-21T00:00:00Z").UnixNano(),
		hash:      sha("key1"),
		key:       []byte("key1"),
		snappyVal: SnappyEncode([]byte(testVal)),
	},
	{
		t:         ts("2014-09-21T00:00:01Z").UnixNano(),
		hash:      sha("key2"),
		key:       []byte("key2"),
		snappyVal: SnappyEncode([]byte(testVal)),
	},
	{
		t:         ts("2014-09-21T00:00:02Z").UnixNano(),
		hash:      sha("key3"),
		key:       []byte("key3"),
		snappyVal: SnappyEncode([]byte(testVal)),
	},
	{
		t:         ts("2014-09-21T00:00:03Z").UnixNano(),
		hash:      sha("key4"),
		key:       []byte("key4"),
		snappyVal: SnappyEncode([]byte(testVal)),
	},
	{
		t:         ts("2014-09-21T00:00:04Z").UnixNano(),
		hash:      sha("key5"),
		key:       []byte("key5"),
		snappyVal: SnappyEncode([]byte(testVal)),
	},
}

const (
	testLog1 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\x5e\x92\x00\x00" + "\x04key1" + "\x13" + testSnappyVal + // record 1
		"\x13\x95\xcb\x4f\x9a\x2c\xca\x00" + "\x04key2" + "\x13" + testSnappyVal // record 2
	testIdx1 = "\336idx\x00\x00\x00\x01" + // header
		"\x01\x10\x73\xab\x6c\xda\x4b\x99\x1c\xd2\x9f\x9e\x83\xa3\x07\xf3\x40\x04\xae\x93\x27\x08" + // record 1
		"\x01\x87\xba\x78\xe0\xf0\x3a\xfc\xef\x60\x65\x7f\x34\x2e\xc5\x56\x73\x68\xfa\xdd\x8c\x21" + // record 2
		"\x00\x4a\x94\x38\x4e\x8f" // trailer
	testLog2 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\xd5\xc7\x94\x00" + "\x04key3" + "\x13" + testSnappyVal + // record 3
		"\x13\x95\xcb\x50\x11\x62\x5e\x00" + "\x04key4" + "\x13" + testSnappyVal // record 4
	testIdx2 = "\336idx\x00\x00\x00\x01" + // header
		"\x01\x3b\x88\xea\x81\x6c\x78\xec\x10\x40\x41\xa7\x5e\x78\xf3\x2e\xc8\x04\xea\xac\x39\x08" + // record 3
		"\x01\xc3\x4b\xf5\xa9\xec\xca\x6e\xdc\x31\x28\x01\x8b\x1d\xd2\x35\xa0\xf7\xbd\xff\x20\x21" + // record 4
		"\x00\x4a\x67\xa6\x75\x19" // trailer
	testLog3 = "\336log\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x50\x4c\xfd\x28\x00" + "\x04key5" + "\x13" + testSnappyVal // record 5
	testIdx3 = "\336idx\x00\x00\x00\x01" + // header
		"\x01\xaf\x06\x5e\x03\xe2\x2f\xe1\xf5\xf9\x5b\x8c\xe9\xf4\x76\x1c\x74\xda\x07\x68\x57\x08" + // record 5
		"\x00\x29\x0b\x3f\xe7\x19" // trailer
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
