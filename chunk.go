package main

import (
	"os"
	"strings"

	"github.com/cespare/kvcache/internal/github.com/edsrzf/mmap-go"
)

type IndexEntry struct {
	hash   keyHash
	offset uint32
}

type WriteChunk struct {
	*WriteLog
	basename string

	// WRONLY files
	idxf *os.File
	logf *os.File

	index         []IndexEntry
	lastTimestamp int64
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

func (wc *WriteChunk) WriteRecord(er *EncodedRecord) (offset uint32, err error) {
	offset, err = wc.WriteLog.WriteRecord(er)
	if err != nil {
		return
	}
	wc.index = append(wc.index, IndexEntry{er.hash, offset})
	if er.t > wc.lastTimestamp {
		wc.lastTimestamp = er.t
	}
	return offset, nil
}

func (wc *WriteChunk) Close() error {
	return wc.WriteLog.Close() // takes care of closing wc.idxf and wc.logf
}

func (wc *WriteChunk) ReopenAsReadChunk() (*ReadChunk, error) {
	if err := wc.Close(); err != nil {
		return nil, err
	}
	return OpenReadChunk(wc.basename, wc.index)
}

type ReadChunk struct {
	*ReadLog
	f             *os.File // RDONLY
	m             mmap.MMap
	index         []IndexEntry
	lastTimestamp int64
}

func LoadReadChunk(basename string) (index []IndexEntry, rc *ReadChunk, err error) {
	f, err := os.Open(basename + ".idx")
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	index, logSize, err := ParseIndex(f)
	if err != nil {
		return nil, nil, err
	}

	f, err = os.Open(basename + ".log")
	if err != nil {
		return nil, nil, err
	}
	if err := VerifyLog(f, logSize); err != nil {
		return nil, nil, err
	}

	rc, err = newReadChunk(f, index)
	if err != nil {
		return nil, nil, err
	}
	return index, rc, nil
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
	rc := &ReadChunk{
		f:       f,
		m:       m,
		ReadLog: OpenReadLog([]byte(m)),
		index:   index,
	}
	// Set the lastTimestamp by looking at the last record in the chunk
	if len(index) > 0 {
		record, err := rc.ReadRecord(index[len(index)-1].offset)
		if err != nil {
			return nil, err
		}
		rc.lastTimestamp = record.t
	}
	return rc, nil
}

func (rc *ReadChunk) Close() error {
	if err := rc.m.Unmap(); err != nil {
		return err
	}
	return rc.f.Close()
}

// Filenames returns the names of all files associated with rc.
func (rc *ReadChunk) Filenames() []string {
	basename := strings.TrimSuffix(rc.f.Name(), ".log")
	return []string{basename + ".idx", basename + ".log"}
}
