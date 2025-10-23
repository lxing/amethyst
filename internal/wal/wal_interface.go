package wal

import (
	"bytes"
	"context"

	"amethyst/internal/common"
)

// Equal compares two entries using slice content rather than pointer identity.
func Equal(a, b *common.Entry) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Type == b.Type && a.Seq == b.Seq && bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
	}
}

// WAL defines the minimal contract required by the DB layer to persist
// and recover write operations.
type WAL interface {
	Append(ctx context.Context, batch []*common.Entry) error
	Iterator(ctx context.Context) (WALIterator, error)
}

// WALIterator walks entries recovered from the log.
// Next returns nil, nil when EOF is reached.
type WALIterator interface {
	Next() (*common.Entry, error)
	Close() error
}
