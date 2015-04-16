package main

import (
	"crypto/sha1"
	"strconv"
	"testing"

	"github.com/cespare/kvcache/internal/github.com/cespare/asrt"
)

func TestRefMap(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping expensive refmap test")
		t.SkipNow()
	}
	var keys [][20]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, sha1.Sum([]byte(strconv.Itoa(i))))
	}
	m := NewRefMap()
	for i, key := range keys {
		exists := m.Put(key, RecordRef{uint32(i), uint32(i)})
		asrt.Equal(t, exists, false)
	}
	for i := 0; i < 3; i++ {
		switch i {
		case 1:
			m.resize(m.bucketSize + 1)
		case 2:
			m.resize(m.bucketSize - 1)
		}
		for j, key := range keys {
			v, ok := m.Get(key)
			asrt.Equal(t, ok, true)
			asrt.Equal(t, v, RecordRef{uint32(j), uint32(j)})
		}
	}
	half := len(keys) / 2
	for _, key := range keys[:half] {
		ok := m.Delete(key)
		asrt.Equal(t, ok, true)
	}
	for _, key := range keys[:half] {
		ok := m.Delete(key)
		asrt.Equal(t, ok, false)
	}
	for i, key := range keys[half:] {
		v, ok := m.Get(key)
		asrt.Equal(t, ok, true)
		asrt.Equal(t, v, RecordRef{uint32(i + half), uint32(i + half)})
	}
}
