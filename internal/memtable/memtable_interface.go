package memtable

import "amethyst/internal/common"

// Memtable defines the interface for a memory-backed key-value store.
// Sequence numbers must be provided by the caller (from DB's global sequence counter).
type Memtable interface {
	Put(key, value []byte, seq uint32)
	Delete(key []byte, seq uint32)
	Get(key []byte) (*common.Entry, bool)
	Iterator() common.EntryIterator
	Len() int
}
