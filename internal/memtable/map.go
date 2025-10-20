package memtable

import "bytes"

// MapMemtable is the baseline Go map-backed implementation.
type MapMemtable struct {
	items map[string]Entry
}

// NewMemtable returns the default map-backed memtable.
func NewMemtable() Memtable {
	return &MapMemtable{
		items: make(map[string]Entry),
	}
}

// Put records or overwrites a key/value pair with the provided sequence number.
func (m *MapMemtable) Put(seq uint64, key, value []byte) error {
	m.items[string(key)] = Entry{
		Sequence:  seq,
		Value:     bytes.Clone(value),
		Tombstone: false,
	}
	return nil
}

// Delete installs a tombstone for the given key.
func (m *MapMemtable) Delete(seq uint64, key []byte) error {
	m.items[string(key)] = Entry{
		Sequence:  seq,
		Tombstone: true,
	}
	return nil
}

// Get returns the most recent entry for key, if any.
func (m *MapMemtable) Get(key []byte) (Entry, bool) {
	entry, ok := m.items[string(key)]
	return entry, ok
}
