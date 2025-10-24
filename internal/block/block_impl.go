package block

import "amethyst/internal/common"

// blockImpl parses and stores all entries from a data block for fast lookups.
type blockImpl struct {
	entries []*common.Entry // sorted by key
}

// NewBlock parses a raw data block into memory.
func NewBlock(data []byte) (Block, error) {
	// TODO: Parse all entries from data into entries slice
	return &blockImpl{}, nil
}

// Get performs binary search to find the entry for the given key.
func (b *blockImpl) Get(key []byte) (*common.Entry, error) {
	// TODO: Binary search on b.entries
	return nil, nil
}
