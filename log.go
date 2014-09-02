package main

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
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

type Record struct {
	t     time.Time
	key   []byte
	value []byte
}


type WriteLog struct {
	f       *os.File    // Opened WRONLY
	crc     hash.Hash32 // Running CRC-32 checksum
	size    uint64
	maxSize uint64
}

func NewWriteLog(filename string, maxSize uint64) (*WriteLog, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	w := &WriteLog{
		f:       f,
		crc:     crc32.NewIEEE(),
		maxSize: maxSize,
	}
	header := make([]byte, 8)
	copy(header, "k\336vs")
	binary.BigEndian.PutUint32(header[4:], 1)
	if _, err := w.Write(header); err != nil {
		return nil, err
	}
	return w, nil
}

var ErrWriteLogFull = errors.New("write log is filled to max capacity")

func (w *WriteLog) WriteRecord(rec *Record) (offset uint64, err error) {
	// TODO: Come up with some hard upper bounds for key and value sizes and enforce above this in the database.
	// Then it's ok to ignore the fact that this method panics for very large keys/values (when the uvarint
	// sizes take > 8 bytes).
	if w.size >= w.maxSize {
		return 0, ErrWriteLogFull
	}
	sizeBefore := w.size
	scratch := make([]byte, 8+8+len(rec.key)+8) // Upper-bound guess
	binary.BigEndian.PutUint64(scratch, uint64(rec.t.UnixNano()))
	nk := binary.PutUvarint(scratch[8:], uint64(len(rec.key)))
	copy(scratch[8+nk:], rec.key)
	nv := binary.PutUvarint(scratch[8+nk+len(rec.key):], uint64(len(rec.value)))
	scratch = scratch[:8+nk+len(rec.key)+nv]
	if _, err := w.Write(scratch); err != nil {
		return 0, err
	}
	flateWriter, err := flate.NewWriter(w, flate.DefaultCompression)
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

func (w *WriteLog) Close() error {
	sum := w.crc.Sum(nil)
	if _, err := w.f.Write(sum); err != nil {
		return err
	}
	return w.f.Close()
}

func (w *WriteLog) Write(b []byte) (n int, err error) {
	n, err = w.f.Write(b)
	w.crc.Write(b[:n])
	w.size += uint64(n)
	return
}

type ReadLog struct {
	f *os.File  // Opened RDONLY
	m mmap.MMap // RDONLY mmap
}

func OpenReadLog(filename string) (*ReadLog, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &ReadLog{
		f: f,
		m: m,
	}, nil
}

func (w *WriteLog) ReopenAsReadLog() (*ReadLog, error) {
	if err := w.Close(); err != nil {
		return nil, err
	}
	return OpenReadLog(w.f.Name())
}

var (
	ErrBadRecordKeyLen   = errors.New("got bad value (or could not read) for record key length")
	ErrBadRecordValueLen = errors.New("got bad value (or could not read) for record value length")
)

func (r *ReadLog) ReadRecord(offset uint64) (*Record, error) {
	// TODO: Same sanity checking here as for WriteRecord.
	b := r.m[offset:]
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

func (r *ReadLog) ReadFirstRecord() (*Record, error) {
	return r.ReadRecord(8)
}

func (r *ReadLog) Close() error {
	if err := r.m.Unmap(); err != nil {
		return err
	}
	return r.f.Close()
}

func (r *ReadLog) Filename() string {
	return r.f.Name()
}
