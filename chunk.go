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
	basename string

	// WRONLY files
	idxf *os.File
	logf *os.File

	index         []IndexEntry
	lastTimestamp time.Time
}

func NewWriteChunk(basename string, maxSize uint64) (*WriteChunk, error) {
	idxf, err := os.OpenFile(basename+".idx", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	logf, err := os.OpenFile(basename+".log", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	log, err := NewWriteLog(idxf, logf, maxSize)
	if err != nil {
		return nil, err
	}
	return &WriteChunk{
		basename: basename,
		idxf:     idxf,
		logf:     logf,
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
	return wb.WriteLog.Close() // takes care of closing wb.idxf and wb.logf
}

func (wb *WriteChunk) ReopenAsReadChunk() (*ReadChunk, error) {
	if err := wb.Close(); err != nil {
		return nil, err
	}
	return OpenReadChunk(wb.basename, wb.index)
}

type ReadChunk struct {
	*ReadLog
	f             *os.File // RDONLY
	m             mmap.MMap
	index         []IndexEntry
	lastTimestamp time.Time
}

func LoadReadChunk(basename string) (*ReadChunk, error) {
	f, err := os.Open(basename + ".idx")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	index, logSize, err := ParseIndex(f)
	if err != nil {
		return nil, err
	}

	f, err = os.Open(basename + ".log")
	if err != nil {
		return nil, err
	}
	if err := VerifyLog(f, logSize); err != nil {
		return nil, err
	}

	return newReadChunk(f, index)
}

func OpenReadChunk(basename string, index []IndexEntry) (*ReadChunk, error) {
	f, err := os.Open(basename + ".log")
	if err != nil {
		return nil, err
	}
	return newReadChunk(f, index)
}

func newReadChunk(f *os.File, index []IndexEntry) (*ReadChunk, error) {
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
