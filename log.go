package main

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"time"
)

// This file implements a rotating, append-only log of expiring key/value pairs.

// The format of these logs is vaguely similar to git's pack file format.
//
// The general format of a logfile is
// <header><record 0><record 1>...<record N><checksum>
//
// All multi-byte sequences are stored in network byte order (big endian).
//
// The header consists of:
//
// - A 4-byte magic sequence: "k\336vs" (in decimal: [107, 222, 118, 115])
// - A 4-byte version number (this is version 1)
//
// A record consists of:
//
// - 8-byte (uint64) nanosecond timestamp (monotonically increasing within the file)
// - uvarint-encoded key size, K
// - K bytes for the key
// - uvarint-encoded *uncompressed* value size, V_dec
// - V_enc bytes of DEFLATE-compressed data for the value (where V_enc is different from -- and hopefully
//   smaller than -- V_dec).
//
// The checksum is the 4-byte IEEE CRC-32 checksum of everything preceding it in the file.

// The general flow is that a new WriteLog is opened and records are fed in until the maximum size is reached.
// Then the file is closed and reopened as a ReadLog.

// TODO: Periodic fsync?

// TODO: Reindexing a log (that is, reading a log and constructing the b-tree of keys and offsets, which we
// might do on server boot, for instance) must necessarily involve reading the whole file and inflating all
// the values. If this proves to be slow (likely) we can make it significantly faster by writing out an
// auxiliary index file as we go, which only contains timestamps, keys, and value offsets. (This is analagous
// to git's pack indexes.)

type WriteLog struct {
	w       io.WriteCloser
	crc     hash.Hash32 // Running CRC-32 checksum
	size    uint64
	maxSize uint64
}

func NewWriteLog(w io.WriteCloser, maxSize uint64) (*WriteLog, error) {
	wl := &WriteLog{
		w:       w,
		crc:     crc32.NewIEEE(),
		maxSize: maxSize,
	}
	header := make([]byte, 8)
	copy(header, "k\336vs")
	binary.BigEndian.PutUint32(header[4:], 1)
	if _, err := wl.Write(header); err != nil {
		return nil, err
	}
	return wl, nil
}

var ErrWriteLogFull = errors.New("write log is filled to max capacity")

func (wl *WriteLog) WriteRecord(rec *Record) (offset uint64, err error) {
	// TODO: Come up with some hard upper bounds for key and value sizes and enforce above this in the database.
	// Then it's ok to ignore the fact that this method panics for very large keys/values (when the uvarint
	// sizes take > 8 bytes).
	if wl.size >= wl.maxSize {
		return 0, ErrWriteLogFull
	}
	sizeBefore := wl.size
	scratch := make([]byte, 8+8+len(rec.key)+8) // Upper-bound guess
	binary.BigEndian.PutUint64(scratch, uint64(rec.t.UnixNano()))
	nk := binary.PutUvarint(scratch[8:], uint64(len(rec.key)))
	copy(scratch[8+nk:], rec.key)
	nv := binary.PutUvarint(scratch[8+nk+len(rec.key):], uint64(len(rec.value)))
	scratch = scratch[:8+nk+len(rec.key)+nv]
	if _, err := wl.Write(scratch); err != nil {
		return 0, err
	}
	flateWriter, err := flate.NewWriter(wl, flate.DefaultCompression)
	if err != nil {
		panic("flate.NewWriter failed")
	}
	if _, err := flateWriter.Write(rec.value); err != nil {
		return 0, err
	}
	if err := flateWriter.Close(); err != nil {
		return 0, err
	}
	return sizeBefore, nil
}

func (wl *WriteLog) Close() error {
	sum := wl.crc.Sum(nil)
	if _, err := wl.w.Write(sum); err != nil {
		return err
	}
	return wl.w.Close()
}

func (wl *WriteLog) Write(b []byte) (n int, err error) {
	n, err = wl.w.Write(b)
	wl.crc.Write(b[:n])
	wl.size += uint64(n)
	return
}

type ReadLog struct {
	b []byte
}

func OpenReadLog(b []byte) *ReadLog { return &ReadLog{b} }

var (
	ErrBadRecordKeyLen   = errors.New("got bad value (or could not read) for record key length")
	ErrBadRecordValueLen = errors.New("got bad value (or could not read) for record value length")
)

func (rl *ReadLog) ReadRecord(offset uint64) (*Record, error) {
	// TODO: Same sanity checking here as for WriteRecord.
	b := rl.b[offset:]
	t := time.Unix(0, int64(binary.BigEndian.Uint64(b[:8])))

	nk, n := binary.Uvarint(b[8:])
	if n <= 0 {
		return nil, ErrBadRecordKeyLen
	}
	key := make([]byte, nk)
	copy(key, b[8+n:8+n+int(nk)])

	b = b[8+n+int(nk):]
	nv, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, ErrBadRecordValueLen
	}
	value := make([]byte, nv)
	flateReader := flate.NewReader(bytes.NewReader(b[n:]))
	defer flateReader.Close()
	if _, err := io.ReadFull(flateReader, value); err != nil {
		return nil, err
	}

	return &Record{
		t:     t,
		key:   key,
		value: value,
	}, nil
}

func (rl *ReadLog) ReadFirstRecord() (*Record, error) { return rl.ReadRecord(8) }
