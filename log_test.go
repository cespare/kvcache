package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/cespare/asrt"
)

func TestReadWriteLog(t *testing.T) {
	buf := buffer{new(bytes.Buffer)}
	wl, err := NewWriteLog(&buf, 80) // Big enough for two records only.
	asrt.Equal(t, err, nil)

	const (
		value       = "this is the value"
		snappyValue = "\x11@this is the value" // snappy encoding of value
		headerLen   = 8
		recordLen   = 8 + 1 + 4 + 1 + len(snappyValue) // 33
	)

	// Write some values
	offset, err := wl.WriteRecord(&Record{
		t:     ts("2014-09-21T00:00:00Z"),
		key:   []byte("key1"),
		value: []byte(value),
	})
	asrt.Equal(t, err, nil)
	asrt.Equal(t, int(offset), headerLen)

	offset, err = wl.WriteRecord(&Record{
		t:     ts("2014-09-21T00:00:01Z"),
		key:   []byte("key2"),
		value: []byte(value),
	})
	asrt.Equal(t, err, nil)
	asrt.Equal(t, int(offset), headerLen+recordLen)

	// We can write one more record because we don't declare wl full until we're over the max size.
	offset, err = wl.WriteRecord(&Record{
		t:     ts("2014-09-21T00:00:02Z"),
		key:   []byte("key3"),
		value: []byte(value),
	})
	asrt.Equal(t, err, nil)
	asrt.Equal(t, int(offset), headerLen+recordLen+recordLen)

	offset, err = wl.WriteRecord(&Record{
		t:     ts("2014-09-21T00:00:03Z"),
		key:   []byte("key4"),
		value: []byte(value),
	})
	asrt.Equal(t, err, ErrWriteLogFull)

	err = wl.Close()
	asrt.Equal(t, err, nil)

	// Check the written log
	want := "k\336vs\x00\x00\x00\x01" + // header
		"\x13\x95\xcb\x4f\x5e\x92\x00\x00" + "\x04key1" + "\x13" + snappyValue + // record 1
		"\x13\x95\xcb\x4f\x9a\x2c\xca\x00" + "\x04key2" + "\x13" + snappyValue + // record 2
		"\x13\x95\xcb\x4f\xd5\xc7\x94\x00" + "\x04key3" + "\x13" + snappyValue + // record 3
		"\x19\x78\xa5\x11" // crc32
	asrt.Equal(t, buf.String(), want)

	// Reading
	rl := OpenReadLog(buf.Bytes())

	rec, err := rl.ReadRecord(headerLen)
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:00Z"))
	asrt.Equal(t, string(rec.key), "key1")
	asrt.Equal(t, string(rec.value), value)

	rec, err = rl.ReadRecord(uint64(headerLen + recordLen))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:01Z"))
	asrt.Equal(t, string(rec.key), "key2")
	asrt.Equal(t, string(rec.value), value)

	rec, err = rl.ReadRecord(uint64(headerLen + recordLen + recordLen))
	asrt.Equal(t, err, nil)
	asrt.Equal(t, rec.t, ts("2014-09-21T00:00:02Z"))
	asrt.Equal(t, string(rec.key), "key3")
	asrt.Equal(t, string(rec.value), value)
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
