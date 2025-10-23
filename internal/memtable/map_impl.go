package memtable

import (
	"sort"
	"sync"

	"amethyst/internal/common"
)

// MapMemtableImpl is the baseline Go map-backed implementation.
type MapMemtableImpl struct {
	mu    sync.RWMutex
	items map[string]*common.Entry
	next  uint64
}

// NewMapMemtable returns the default map-backed memtable implementation.
func NewMapMemtable() Memtable {
	return &MapMemtableImpl{
		items: make(map[string]*common.Entry),
	}
}

// Put records or overwrites a key/value pair using the provided key and value.
func (m *MapMemtableImpl) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.next++
	m.items[string(key)] = &common.Entry{
		Type:  common.EntryTypePut,
		Seq:   m.next,
		Value: cloneBytes(value),
	}
	return nil
}

// Delete installs a tombstone for the given key.
func (m *MapMemtableImpl) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.next++
	m.items[string(key)] = &common.Entry{
		Type: common.EntryTypeDelete,
		Seq:  m.next,
	}
	return nil
}

// Get returns the most recent value for key, if any.
func (m *MapMemtableImpl) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	entry, ok := m.items[string(key)]
	if !ok || entry.Type != common.EntryTypePut {
		m.mu.RUnlock()
		return nil, false
	}
	value := cloneBytes(entry.Value)
	m.mu.RUnlock()
	return value, true
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
		entries = append(entries, cloneIteratorEntry(m.items[k], k))
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

func cloneIteratorEntry(src *common.Entry, key string) *common.Entry {
	if src == nil {
		return nil
	}
	entry := &common.Entry{
		Type: src.Type,
		Seq:  src.Seq,
		Key:  cloneBytes([]byte(key)),
	}
	if src.Type == common.EntryTypePut {
		entry.Value = cloneBytes(src.Value)
	}
	return entry
}
