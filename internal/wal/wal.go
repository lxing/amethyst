package wal

import "context"

// EntryType enumerates the kinds of operations persisted in the WAL.
type EntryType uint8

const (
	EntryTypePut EntryType = iota
	EntryTypeDelete
)

// Entry captures the durable form written to and replayed from the WAL.
type Entry struct {
	Type  EntryType
	Seq   uint64
	Key   []byte
	Value []byte
}

// Log defines the minimal contract required by the DB layer to persist
// and recover write operations. Implementations can extend this surface
// with batching or sync controls later without changing existing code.
type Log interface {
	Append(ctx context.Context, e Entry) error
	Replay(ctx context.Context, fn func(Entry) error) error
}
