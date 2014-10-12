package main

import (
	"hash"
	"hash/crc32"
	"io"
)

// A bufWriteCloser is an io.WriteCloser that buffers its writes until Flush is called.
// The internal buffer is reused after each flush, so it is always as large as it has ever grown.
type bufWriteCloser struct {
	wc  io.WriteCloser
	b   []byte
	err error
}

func newBufWriteCloser(wc io.WriteCloser) *bufWriteCloser {
	return &bufWriteCloser{wc: wc}
}

func (w *bufWriteCloser) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, err
	}
	w.b = append(w.b, p...)
	return len(p), nil
}

func (w *bufWriteCloser) Buffered() int { return len(w.b) }

func (w *bufWriteCloser) Flush() error {
	if w.err != nil {
		return w.err
	}
	_, err := w.wc.Write(w.b)
	if err != nil {
		w.err = err
		return err
	}
	w.b = w.b[:0]
	return nil
}

func (w *bufWriteCloser) Close() error {
	err := w.Flush()
	err2 := w.wc.Close()
	if err != nil {
		return err
	}
	return err2
}

// A sizeWriteCloser is an io.WriteCloser that tracks its written size.
type sizeWriteCloser struct {
	io.WriteCloser
	size uint64
}

func newSizeWriteCloser(w io.WriteCloser) *sizeWriteCloser {
	return &sizeWriteCloser{WriteCloser: w}
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

func newCRCWriteCloser(w io.WriteCloser) *crcWriteCloser {
	return &crcWriteCloser{WriteCloser: w, crc: crc32.NewIEEE()}
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
