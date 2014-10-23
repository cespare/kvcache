package main

const (
	// bucketBytes is the number of bytes of the key used to determine the hash bucket.
	// If we wanted to dynamically scale through many different orders of magnitude,
	// we'd need to adjust bucketBytes over time. But this stays static for now,
	// so this hashtable is meant for holding 10s of millions of entries.
	bucketBytes = 3
	numBuckets  = 1 << (8 * bucketBytes)
	// initialBucketSize determines the number of slots to place at each bucket position initially
	// (this grows as the table is resized).
	initialBucketSize = 3
	minLoadFactor     = 0.35
	maxLoadFactor     = 0.65
)

type RefMapEntry struct {
	occ bool
	key [20]byte
	val RecordRef
}

// A RefMap maps SHA-1 key hashes ([20]byte) to RecordRefs.
// It is implemented as an open-addressed hash table with linear probing.
// It is resized when the the load factor grows too high.
// Using this instead of a Go map type avoids GC penalties from using very large maps.
type RefMap struct {
	slots      []RefMapEntry
	bucketSize int
	len        int
	cap        int
}

func newRefMap(bucketSize int) *RefMap {
	n := numBuckets * bucketSize
	return &RefMap{
		slots:      make([]RefMapEntry, n),
		bucketSize: bucketSize,
		cap:        n,
	}
}

func NewRefMap() *RefMap {
	return newRefMap(initialBucketSize)
}

func (m *RefMap) Put(key [20]byte, val RecordRef) (exists bool) {
	for i := m.slotIdx(key); ; i = (i + 1) % m.cap {
		if !m.slots[i].occ {
			// We've found an empty slot to put this entry.
			m.slots[i] = RefMapEntry{
				occ: true,
				key: key,
				val: val,
			}
			m.len++
			if m.load() > maxLoadFactor {
				m.resize(m.bucketSize + 1)
			}
			return false
		}
		if key == m.slots[i].key {
			m.slots[i].val = val
			return true
		}
	}
}

func (m *RefMap) Get(key [20]byte) (val RecordRef, ok bool) {
	for i := m.slotIdx(key); ; i = (i + 1) % m.cap {
		if !m.slots[i].occ {
			return RecordRef{}, false
		}
		if key == m.slots[i].key {
			return m.slots[i].val, true
		}
	}
}

func (m *RefMap) Delete(key [20]byte) (ok bool) {
	i := m.slotIdx(key)
	for ; ; i = (i + 1) % m.cap {
		if !m.slots[i].occ {
			return false
		}
		if key == m.slots[i].key {
			break
		}
	}
	// We've found our entry: it is at i. When we remove it, there's going to be a gap in the table which could
	// break the probing invariant: for a key k in the hash table, there are no unoccupied slots between k's
	// initial slot index and the location where k resides.
	// So, we'll iterate forwards through the table, moving items into the unoccupied slot when we can.
	// See pseudocode at http://en.wikipedia.org/wiki/Open_addressing
	j := i
	for {
		m.slots[i].occ = false
	findSwappable:
		j = (j + 1) % m.cap
		if !m.slots[j].occ {
			break
		}
		k := m.slotIdx(m.slots[j].key)
		if i <= j && i < k && k <= j {
			goto findSwappable
		}
		if i > j && (i < k || k <= j) {
			goto findSwappable
		}
		// We found an entry that can be swapped into slot i without breaking the probing invariant.
		m.slots[i] = m.slots[j]
		i = j
	}
	m.len--
	if m.bucketSize > initialBucketSize && m.load() < minLoadFactor {
		m.resize(m.bucketSize - 1)
	}
	return true
}

func (m *RefMap) Len() int { return m.len }

func (m *RefMap) slotIdx(k [20]byte) int {
	return (int(k[0])<<16 + int(k[1])<<8 + int(k[2])) * m.bucketSize
}

func (m *RefMap) load() float64 {
	return float64(m.len) / float64(m.cap)
}

func (m *RefMap) resize(newBucketSize int) {
	m2 := newRefMap(newBucketSize)
	for i := range m.slots {
		if m.slots[i].occ {
			m2.Put(m.slots[i].key, m.slots[i].val)
		}
	}
	*m = *m2
}
