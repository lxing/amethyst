package memtable

// Entry describes a single key mutation stored in the memtable.
type Entry struct {
	Sequence  uint64
	Value     []byte
	Tombstone bool
}

// Memtable defines the minimal interface required by the DB layer.
type Memtable interface {
	Put(seq uint64, key, value []byte) error
	Delete(seq uint64, key []byte) error
	Get(key []byte) (Entry, bool)
}
