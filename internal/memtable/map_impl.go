package memtable

import (
	"bytes"
	"sort"
	"sync"

	"amethyst/internal/common"
)

// MapMemtableImpl is the baseline Go map-backed implementation.
type MapMemtableImpl struct {
	mu    sync.RWMutex
	items map[string]MemtableEntry
}

// NewMapMemtable returns the default map-backed memtable implementation.
func NewMapMemtable() Memtable {
	return &MapMemtableImpl{
		items: make(map[string]MemtableEntry),
	}
}

// Put records or overwrites a key/value pair with the provided sequence number.
func (m *MapMemtableImpl) Put(seq uint64, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items[string(key)] = MemtableEntry{
		Sequence:  seq,
		Value:     bytes.Clone(value),
		Tombstone: false,
	}
	return nil
}

// Delete installs a tombstone for the given key.
func (m *MapMemtableImpl) Delete(seq uint64, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items[string(key)] = MemtableEntry{
		Sequence:  seq,
		Tombstone: true,
	}
	return nil
}

// Get returns the most recent entry for key, if any.
func (m *MapMemtableImpl) Get(key []byte) (MemtableEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.items[string(key)]
	return entry, ok
}

// Iterator returns a stable snapshot iterator over the current entries.
func (m *MapMemtableImpl) Iterator() common.EntryIterator {
	m.mu.RLock()
	keys := make([]string, 0, len(m.items))
	for k := range m.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]*common.Entry, 0, len(keys))
	for _, k := range keys {
		item := m.items[k]
		entryType := common.EntryTypePut
		var value []byte
		if item.Tombstone {
			entryType = common.EntryTypeDelete
		} else {
			value = cloneBytes(item.Value)
		}
		entries = append(entries, &common.Entry{
			Type:  entryType,
			Seq:   item.Sequence,
			Key:   cloneBytes([]byte(k)),
			Value: value,
		})
	}
	m.mu.RUnlock()

	return &memtableIterator{entries: entries}
}

type memtableIterator struct {
	entries []*common.Entry
	index   int
}

func (it *memtableIterator) Next() (*common.Entry, error) {
	if it.index >= len(it.entries) {
		return nil, nil
	}
	entry := it.entries[it.index]
	it.index++
	return entry, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out
}
