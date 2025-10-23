package memtable

import "bytes"

// MapMemtableImpl is the baseline Go map-backed implementation.
type MapMemtableImpl struct {
	items map[string]MemtableEntry
}

// NewMapMemtable returns the default map-backed memtable implementation.
func NewMapMemtable() Memtable {
	return &MapMemtableImpl{items: make(map[string]MemtableEntry)}
}

// Put records or overwrites a key/value pair with the provided sequence number.
func (m *MapMemtableImpl) Put(seq uint64, key, value []byte) error {
	m.items[string(key)] = MemtableEntry{
		Sequence:  seq,
		Value:     bytes.Clone(value),
		Tombstone: false,
	}
	return nil
}

// Delete installs a tombstone for the given key.
func (m *MapMemtableImpl) Delete(seq uint64, key []byte) error {
	m.items[string(key)] = MemtableEntry{
		Sequence:  seq,
		Tombstone: true,
	}
	return nil
}

// Get returns the most recent entry for key, if any.
func (m *MapMemtableImpl) Get(key []byte) (MemtableEntry, bool) {
	entry, ok := m.items[string(key)]
	return entry, ok
}
