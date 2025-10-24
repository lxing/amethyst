package sstable

import "amethyst/internal/common"

// SSTable provides read access to a sorted string table file.
type SSTable interface {
	// Get returns the entry for the given key, or nil if not found.
	Get(key []byte) (*common.Entry, error)

	// Iterator returns an iterator over all entries in the table.
	Iterator() (common.EntryIterator, error)

	// Close releases resources associated with this SSTable.
	Close() error
}
