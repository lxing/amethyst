package sstable

import "amethyst/internal/common"

// SSTable provides read access to a sorted string table file.
type SSTable interface {
	// Get returns the entry for the given key.
	// Returns (entry, true, nil) if found, (nil, false, nil) if not found, or (nil, false, error) on error.
	Get(key []byte) (*common.Entry, bool, error)

	// Iterator returns an iterator over all entries in the table.
	Iterator() common.EntryIterator

	// Close releases resources associated with this SSTable.
	Close() error
}
