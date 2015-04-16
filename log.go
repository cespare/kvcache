package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/cespare/kvcache/internal/github.com/cespare/snappy"
)

// This file implements a rotating, append-only log of expiring key/val pairs.
//
// A log consists of two files of the same name except for different suffixes:
// - A binary log containing the data, named xxx.log, and
// - An index file containing offsets into the log, named xxx.idx.

// The format of these logs is vaguely similar to git's pack file format.
//
// The general format of both filetypes is:
// <header><record 0><record 1>...<record N>[<trailer>]
//
// All multi-byte sequences are stored in network byte order (big endian).
//
// Index format
// ------------
//
// The header consists of:
//
// - A 4-byte magic sequence: "\336idx" (in decimal: [222, 105, 100, 120])
// - A 4-byte version number (this is version 1)
//
// A record consists of:
//
// - extension byte: 1
// - SHA-1 hash of the key (20 bytes)
// - uvarint-encoded offset delta (increasing within the index)
//   For the first index record, this is the offset;
//   for subsequent records, it is the difference from the prior offset.
//
// A trailer consists of:
//
// - extension byte: 0 (to distinguish from a record)
// - uvarint-encoded filesize of the paired log file
// - 4-byte IEEE CRC-32 checksum of everything preceding in this index file.
//
// Log format
// ----------
//
// The log header consists of:
//
// - A 4-byte magic sequence: "\336log" (in decimal: [107, 222, 118, 115])
// - A 4-byte version number (this is version 1)
//   The version number of the log must be identical to that of the index.
//
// A record consists of:
//
// - 8-byte (uint64) nanosecond timestamp  (monotonically increasing within the file)
// - uvarint-encoded key size, K
// - key (K bytes)
// - uvarint-encoded *compressed* value size, V
// - V bytes of snappy-compressed data for the value
//
// The log format has no trailer.

// The general flow is that a new WriteLog is opened and records are fed in
// until the maximum size is reached. Then the file is closed and reopened as a ReadLog.

// The log format is intended to support crash recovery: a partially written index
// can be used to decipher the log. This is not implemented, so for now,
// opening an existing set of logs can only be done when they are all completely written out.
// We attempt to close the write log on shutdown (which includes writing out the index).

const (
	// These constants are used for sanity checking inputs.
	maxKeyLen = 100
	maxValLen = 1e6 // 1MB
)

type WriteLog struct {
	logw       *sizeWriteCloser
	idxw       *crcWriteCloser
	maxSize    uint64
	lastOffset uint32
	scratch    []byte
}

func NewWriteLog(idxWriter, logWriter io.WriteCloser, maxSize uint64) (*WriteLog, error) {
	// The size is an upper bound for everything before the value.
	// It is also large enough to encode the entire index record.
	scratch := make([]byte, 8+8+maxKeyLen+8)

	wl := &WriteLog{
		logw:    &sizeWriteCloser{logWriter, 0},
		idxw:    &crcWriteCloser{idxWriter, crc32.NewIEEE()},
		maxSize: maxSize,
		scratch: scratch,
	}
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[4:], 1)

	// Write index header
	copy(header, "\336idx")
	if _, err := wl.idxw.Write(header); err != nil {
		return nil, err
	}

	// Write log header
	copy(header, "\336log")
	if _, err := wl.logw.Write(header); err != nil {
		return nil, err
	}
	return wl, nil
}

var (
	ErrKeyTooLong   = errors.New("key is too long")
	ErrWriteLogFull = errors.New("write log is filled to max capacity")
)

// WriteRecord inserts a single record into wl. As an optimization, the SHA-1 hash of the key and the Snappy
// encoding of the value are precomputed.
func (wl *WriteLog) WriteRecord(rec *EncodedRecord) (offset uint32, err error) {
	if len(rec.key) > maxKeyLen {
		return 0, ErrKeyTooLong
	}

	size := wl.logw.Size()
	if size >= wl.maxSize {
		return 0, ErrWriteLogFull
	}
	offset = uint32(size)

	// Write to the log
	binary.BigEndian.PutUint64(wl.scratch, uint64(rec.t))
	nk := binary.PutUvarint(wl.scratch[8:], uint64(len(rec.key)))
	copy(wl.scratch[8+nk:], rec.key)

	nv := binary.PutUvarint(wl.scratch[8+nk+len(rec.key):], uint64(len(rec.snappyVal)))
	if _, err := wl.logw.Write(wl.scratch[:8+nk+len(rec.key)+nv]); err != nil {
		return 0, err
	}
	if _, err := wl.logw.Write(rec.snappyVal); err != nil {
		return 0, err
	}

	// Write to the index
	wl.scratch[0] = 1
	copy(wl.scratch[1:], rec.hash[:])

	if offset <= wl.lastOffset {
		panic("offset is smaller than lastOffset")
	}
	off := offset - wl.lastOffset
	wl.lastOffset = offset
	no := binary.PutUvarint(wl.scratch[nk+len(rec.hash):], uint64(off))
	if _, err := wl.idxw.Write(wl.scratch[:nk+len(rec.hash)+no]); err != nil {
		return 0, err
	}

	return offset, nil
}

func (wl *WriteLog) Close() error {
	if err := wl.logw.Close(); err != nil {
		return err
	}
	scratch := make([]byte, 9)
	scratch[0] = 0
	n := binary.PutUvarint(scratch[1:], wl.logw.Size())
	if _, err := wl.idxw.Write(scratch[:n+1]); err != nil {
		return err
	}
	if _, err := wl.idxw.Write(wl.idxw.Sum()); err != nil {
		return err
	}
	return wl.idxw.Close()
}

var (
	ErrBadMagic         = errors.New("log/index file had a bad magic value")
	ErrBadVersion       = errors.New("log/index file had a version not equal to 1")
	ErrIncompleteIndex  = errors.New("index file is incomplete")
	ErrChecksumMismatch = errors.New("index checksum does not match contents")
	ErrExtraContent     = errors.New("junk data at end of index file")
	ErrOffsetTooLarge   = errors.New("found offset too large")
)

func ParseIndex(r io.Reader) (index []IndexEntry, logSize uint64, err error) {
	br := bufio.NewReader(r)
	defer func() {
		if err == io.EOF {
			err = ErrIncompleteIndex
		}
	}()
	crc := crc32.NewIEEE()
	var header []byte
	header, err = checkHeader(br, "\336idx")
	if err != nil {
		return
	}
	crc.Write(header)

	var offset uint32
	for {
		// Read the key
		var c byte
		c, err = br.ReadByte()
		if err != nil {
			return
		}
		crc.Write([]byte{c})
		if c == 0 {
			break
		}
		var hash [20]byte
		if _, err = io.ReadFull(br, hash[:]); err != nil {
			return
		}
		crc.Write(hash[:])

		// Read the offset
		var off64 uint64
		byteRdr := newByteReader(br)
		off64, err = binary.ReadUvarint(byteRdr)
		if err != nil {
			return
		}
		if off64 > math.MaxUint32 {
			err = ErrOffsetTooLarge
			return
		}
		offset += uint32(off64)
		crc.Write(byteRdr.Bytes())

		index = append(index, IndexEntry{
			hash:   hash,
			offset: offset,
		})
	}

	// Read log size
	byteRdr := newByteReader(br)
	logSize, err = binary.ReadUvarint(byteRdr)
	if err != nil {
		return
	}
	crc.Write(byteRdr.Bytes())

	// Read and verify CRC
	checksum := make([]byte, 4)
	if _, err = io.ReadFull(br, checksum); err != nil {
		return
	}
	if !bytes.Equal(crc.Sum(nil), checksum) {
		return nil, 0, ErrChecksumMismatch
	}
	if _, err = br.ReadByte(); err != io.EOF {
		return nil, 0, ErrExtraContent
	}
	return index, logSize, nil
}

var (
	ErrSizeMismatch = errors.New("log size does not match the index")
)

func VerifyLog(r io.ReadSeeker, size uint64) error {
	_, err := checkHeader(r, "\336log")
	if err != nil {
		return err
	}
	n, err := r.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	if _, err := r.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	if uint64(n) != size {
		return ErrSizeMismatch
	}
	return nil
}

func checkHeader(r io.Reader, magic string) (header []byte, err error) {
	header = make([]byte, 8)
	if _, err = io.ReadFull(r, header); err != nil {
		return
	}
	if string(header[:4]) != magic {
		return nil, ErrBadMagic
	}
	if string(header[4:]) != "\x00\x00\x00\x01" {
		return nil, ErrBadVersion
	}
	return header, nil
}

type ReadLog struct {
	b []byte
}

func OpenReadLog(b []byte) *ReadLog { return &ReadLog{b} }

var (
	ErrBadRecordKeyLen = errors.New("got bad value (or could not read) for record key length")
	ErrBadRecordValLen = errors.New("got bad value (or could not read) for record value length")
)

func (rl *ReadLog) ReadRecord(offset uint32) (*Record, error) {
	b := rl.b[offset:]
	t := int64(binary.BigEndian.Uint64(b[:8]))

	nk, n := binary.Uvarint(b[8:])
	if n <= 0 || nk > maxKeyLen {
		return nil, ErrBadRecordKeyLen
	}
	key := make([]byte, nk)
	copy(key, b[8+n:8+n+int(nk)])

	b = b[8+n+int(nk):]
	nv, n := binary.Uvarint(b)
	if n <= 0 || nv > maxValLen {
		return nil, ErrBadRecordValLen
	}

	val, err := snappy.Decode(nil, b[n:n+int(nv)])
	if err != nil {
		return nil, err
	}
	// []byte{} is encoded as the single 0 byte, which decodes as nil. Special-case.
	if val == nil {
		val = []byte{}
	}

	return &Record{
		t:   t,
		key: key,
		val: val,
	}, nil
}

// A sizeWriteCloser is an io.WriteCloser that tracks its written size.
type sizeWriteCloser struct {
	io.WriteCloser
	size uint64
}

func (sw *sizeWriteCloser) Size() uint64 { return sw.size }

func (sw *sizeWriteCloser) Write(b []byte) (n int, err error) {
	n, err = sw.WriteCloser.Write(b)
	sw.size += uint64(n)
	return
}

// A crcWriteCloser is an io.WriteCloser that maintains an IEEE CRC-32 checksum of the written contents.
type crcWriteCloser struct {
	io.WriteCloser
	crc hash.Hash32 // Running CRC-32 checksum of the index
}

func (cw *crcWriteCloser) Write(b []byte) (n int, err error) {
	n, err = cw.WriteCloser.Write(b)
	cw.crc.Write(b[:n])
	return
}

func (cw *crcWriteCloser) Sum() []byte {
	return cw.crc.Sum(nil)
}

// A byteReader is an io.ByteReader that remembers what bytes it has read.
type byteReader struct {
	r io.ByteReader
	b []byte
}

func newByteReader(r io.ByteReader) *byteReader {
	return &byteReader{r, nil}
}

func (br *byteReader) ReadByte() (byte, error) {
	c, err := br.r.ReadByte()
	if err != nil {
		return 0, err
	}
	br.b = append(br.b, c)
	return c, nil
}

func (br *byteReader) Bytes() []byte {
	return br.b
}
