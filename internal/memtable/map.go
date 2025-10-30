package memtable

import (
	"sort"
	"sync"

	"amethyst/internal/common"
)

// mapMemtableImpl is the baseline Go map-backed implementation.
// Thread-safe for concurrent reads and writes.
type mapMemtableImpl struct {
	mu    sync.RWMutex
	items map[string]*common.Entry
	next  uint32
}

var _ Memtable = (*mapMemtableImpl)(nil)

// NewMapMemtable returns the default map-backed memtable implementation.
func NewMapMemtable() Memtable {
	return &mapMemtableImpl{
		items: make(map[string]*common.Entry),
	}
}

// Put records or overwrites a key/value pair using the provided key and value.
func (m *mapMemtableImpl) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	m.items[string(key)] = &common.Entry{
		Type:  common.EntryTypePut,
		Seq:   m.next,
		Value: value,
	}
}

// Delete installs a tombstone for the given key.
func (m *mapMemtableImpl) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	m.items[string(key)] = &common.Entry{
		Type: common.EntryTypeDelete,
		Seq:  m.next,
	}
}

// Get returns the most recent entry for key, if any.
func (m *mapMemtableImpl) Get(key []byte) (*common.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.items[string(key)]
	if !ok {
		return nil, false
	}
	// Clone the entry with the key included
	return &common.Entry{
		Type:  entry.Type,
		Seq:   entry.Seq,
		Key:   key,
		Value: entry.Value,
	}, true
}

// Iterator returns a stable snapshot iterator over the current entries.
func (m *mapMemtableImpl) Iterator() common.EntryIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.items))
	for k := range m.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]*common.Entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, cloneIteratorEntry(m.items[k], k))
	}

	return &memtableIterator{entries: entries}
}

// Len returns the number of entries in the memtable.
func (m *mapMemtableImpl) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
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

func cloneIteratorEntry(src *common.Entry, key string) *common.Entry {
	if src == nil {
		return nil
	}
	return &common.Entry{
		Type:  src.Type,
		Seq:   src.Seq,
		Key:   []byte(key),
		Value: src.Value,
	}
}
