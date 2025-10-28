package memtable

import "amethyst/internal/common"

// Memtable defines the interface for a memory-backed key-value store.
type Memtable interface {
	Put(key, value []byte)
	Delete(key []byte)
	Get(key []byte) (*common.Entry, bool)
	Iterator() common.EntryIterator
	Len() int
}
