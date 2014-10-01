package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/cespare/asrt"
)

const (
	testValue       = "this is the value"
	testSnappyValue = "\x11@this is the value" // snappy encoding of testValue
)

var testRecords = []*Record{
	{
		t:     ts("2014-09-21T00:00:00Z"),
		key:   []byte("key1"),
		value: []byte(testValue),
	},
	{
		t:     ts("2014-09-21T00:00:01Z"),
		key:   []byte("key2"),
		value: []byte(testValue),
	},
	{
		t:     ts("2014-09-21T00:00:02Z"),
		key:   []byte("key3"),
		value: []byte(testValue),
	},
	{
		t:     ts("2014-09-21T00:00:03Z"),
		key:   []byte("key4"),
		value: []byte(testValue),
	},
	{
		t:     ts("2014-09-21T00:00:04Z"),
		key:   []byte("key5"),
		value: []byte(testValue),
	},
}

const (
	testLog1 = "k\336vs\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\x5e\x92\x00\x00" + "\x04key1" + "\x13" + testSnappyValue + // record 1
		"\x13\x95\xcb\x4f\x9a\x2c\xca\x00" + "\x04key2" + "\x13" + testSnappyValue + // record 2
		"\xec\x5a\x07\x69"
	testLog2 = "k\336vs\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\xd5\xc7\x94\x00" + "\x04key3" + "\x13" + testSnappyValue + // record 3
		"\x13\x95\xcb\x50\x11\x62\x5e\x00" + "\x04key4" + "\x13" + testSnappyValue + // record 4
		"\xb2\x1e\xd8\x14"
	testLog3 = "k\336vs\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x50\x4c\xfd\x28\x00" + "\x04key5" + "\x13" + testSnappyValue + // record 5
		"\x03\xb8\x73\xec"
)

func TestReadWriteLog(t *testing.T) {
	buf := buffer{new(bytes.Buffer)}
	wl, err := NewWriteLog(&buf, 50) // Big enough for one records only.
	asrt.Equal(t, err, nil)

	const (
		headerLen = 8
		recordLen = 8 + 1 + 4 + 1 + len(testSnappyValue) // 33
	)

	// Write some values
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
	asrt.Equal(t, buf.String(), testLog1)

	// Reading
	rl := OpenReadLog(buf.Bytes())

	rec, err := rl.ReadRecord(headerLen)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:00Z"))
	asrt.Equal(t, string(rec.key), "key1")
	asrt.Equal(t, string(rec.value), testValue)

	rec, err = rl.ReadRecord(uint64(headerLen + recordLen))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:01Z"))
	asrt.Equal(t, string(rec.key), "key2")
	asrt.Equal(t, string(rec.value), testValue)
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
