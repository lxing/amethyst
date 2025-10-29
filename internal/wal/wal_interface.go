package wal

import "amethyst/internal/common"

// WAL defines the minimal contract required by the DB layer to persist
// and recover write operations.
type WAL interface {
	WriteEntry(batch []*common.Entry) error
	Iterator() (common.EntryIterator, error)
	Len() int
	Close() error
}
