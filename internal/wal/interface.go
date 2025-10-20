package wal

import (
	"bytes"
	"context"
)

// EntryType enumerates the kinds of operations persisted in the WAL.
type EntryType uint8

const (
	EntryTypePut    EntryType = iota // 0
	EntryTypeDelete                  // 1
)

// Entry captures the durable form written to and replayed from the WAL.
type Entry struct {
	Type  EntryType
	Seq   uint64
	Key   []byte
	Value []byte
}

// Equal compares two entries using slice content rather than pointer identity.
func (e Entry) Equal(other Entry) bool {
	return e.Type == other.Type && e.Seq == other.Seq && bytes.Equal(e.Key, other.Key) && bytes.Equal(e.Value, other.Value)
}

// WAL defines the minimal contract required by the DB layer to persist
// and recover write operations.
type WAL interface {
	Append(ctx context.Context, batch []Entry) error
	Iterator(ctx context.Context) (WALIterator, error)
}

// WALIterator walks entries recovered from the log.
// Next returns false when EOF is reached.
type WALIterator interface {
	Next() (Entry, bool, error)
	Close() error
}
