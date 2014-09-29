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

type WriteBlock struct {
	*WriteLog
	f             *os.File // WRONLY
	index         []IndexEntry
	lastTimestamp time.Time
}

func NewWriteBlock(filename string, maxSize uint64) (*WriteBlock, error) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	log, err := NewWriteLog(f, maxSize)
	if err != nil {
		return nil, err
	}
	return &WriteBlock{
		f:        f,
		WriteLog: log,
	}, nil
}

func (wb *WriteBlock) WriteRecord(r *Record) (offset uint64, err error) {
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

func (wb *WriteBlock) Close() error {
	return wb.WriteLog.Close() // takes care of closing wb.f
}

func (wb *WriteBlock) ReopenAsReadBlock() (*ReadBlock, error) {
	if err := wb.Close(); err != nil {
		return nil, err
	}
	return OpenReadBlock(wb.f.Name(), wb.index)
}

type ReadBlock struct {
	*ReadLog
	f             *os.File // RDONLY
	m             mmap.MMap
	index         []IndexEntry
	lastTimestamp time.Time
}

func OpenReadBlock(filename string, index []IndexEntry) (*ReadBlock, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	rb := &ReadBlock{
		f:       f,
		m:       m,
		ReadLog: OpenReadLog([]byte(m)),
		index:   index,
	}
	// Set the lastTimestamp by looking at the last record in the block
	if len(index) > 0 {
		record, err := rb.ReadRecord(index[len(index)-1].offset)
		if err != nil {
			return nil, err
		}
		rb.lastTimestamp = record.t
	}
	return rb, nil
}

func (rb *ReadBlock) Close() error {
	if err := rb.m.Unmap(); err != nil {
		return err
	}
	return rb.f.Close()
}

func (rb *ReadBlock) Filename() string {
	return rb.f.Name()
}
