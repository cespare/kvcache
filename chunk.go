package main

import (
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

type IndexEntry struct {
	key    string // string version of the record's key
	offset uint64
}

type WriteChunk struct {
	*WriteLog
	f             *os.File // WRONLY
	index         []IndexEntry
	lastTimestamp time.Time
}

func NewWriteChunk(filename string, maxSize uint64) (*WriteChunk, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	log, err := NewWriteLog(f, maxSize)
	if err != nil {
		return nil, err
	}
	return &WriteChunk{
		f:        f,
		WriteLog: log,
	}, nil
}

func (wb *WriteChunk) WriteRecord(r *Record) (offset uint64, err error) {
	offset, err = wb.WriteLog.WriteRecord(r)
	if err != nil {
		return
	}
	wb.index = append(wb.index, IndexEntry{key: string(r.key), offset: offset})
	if r.t.After(wb.lastTimestamp) {
		wb.lastTimestamp = r.t
	}
	return offset, nil
}

func (wb *WriteChunk) Close() error {
	return wb.WriteLog.Close() // takes care of closing wb.f
}

func (wb *WriteChunk) ReopenAsReadChunk() (*ReadChunk, error) {
	if err := wb.Close(); err != nil {
		return nil, err
	}
	return OpenReadChunk(wb.f.Name(), wb.index)
}

type ReadChunk struct {
	*ReadLog
	f             *os.File // RDONLY
	m             mmap.MMap
	index         []IndexEntry
	lastTimestamp time.Time
}

func OpenReadChunk(filename string, index []IndexEntry) (*ReadChunk, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	rb := &ReadChunk{
		f:       f,
		m:       m,
		ReadLog: OpenReadLog([]byte(m)),
		index:   index,
	}
	// Set the lastTimestamp by looking at the last record in the chunk
	if len(index) > 0 {
		record, err := rb.ReadRecord(index[len(index)-1].offset)
		if err != nil {
			return nil, err
		}
		rb.lastTimestamp = record.t
	}
	return rb, nil
}

func (rb *ReadChunk) Close() error {
	if err := rb.m.Unmap(); err != nil {
		return err
	}
	return rb.f.Close()
}

func (rb *ReadChunk) Filename() string {
	return rb.f.Name()
}
