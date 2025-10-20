package memtable

// Entry describes a single key mutation stored in the memtable.
type Entry struct {
	Sequence  uint64
	Value     []byte
	Tombstone bool
}

// Memtable defines the interface for a memory-backed key-value store.
type Memtable interface {
	Put(seq uint64, key, value []byte) error
	// Delete does not error if the key is missing.
	Delete(seq uint64, key []byte) error
	Get(key []byte) (Entry, bool)
}
