package common

// EntryType enumerates logical operations flowing through WAL, memtable,
// and SSTable components.
type EntryType uint8

const (
	EntryTypePut EntryType = iota
	EntryTypeDelete
)

// Entry captures a single mutation in sequence order.
type Entry struct {
	Type  EntryType
	Seq   uint64
	Key   []byte
	Value []byte
}
