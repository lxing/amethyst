package sstable

import (
	"errors"

	"amethyst/internal/common"
)

var ErrNotFound = errors.New("key not found")

// SSTable provides read access to a sorted string table file.
type SSTable interface {
	// Get returns the entry for the given key.
	// Returns ErrNotFound if the key does not exist.
	Get(key []byte) (*common.Entry, error)

	// Iterator returns an iterator over all entries in the table.
	Iterator() common.EntryIterator

	// Close releases resources associated with this SSTable.
	Close() error
}
