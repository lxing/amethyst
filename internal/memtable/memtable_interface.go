package memtable

import "amethyst/internal/common"

// Memtable defines the interface for a memory-backed key-value store.
type Memtable interface {
	Put(key, value []byte) error
	// Delete does not error if the key is missing.
	Delete(key []byte) error
	Get(key []byte) ([]byte, bool)
	Iterator() common.EntryIterator
}
