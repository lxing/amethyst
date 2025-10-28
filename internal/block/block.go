package block

import (
	"bytes"

	"amethyst/internal/common"
)

// blockImpl parses and stores all entries from a data block for fast lookups.
type blockImpl struct {
	entries []*common.Entry // sorted by key
}

// NewBlock parses a raw data block into memory.
func NewBlock(data []byte) (Block, error) {
	var entries []*common.Entry
	reader := bytes.NewReader(data)

	for {
		entry, err := common.ReadEntry(reader)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break // Clean end of stream
		}
		entries = append(entries, entry)
	}

	return &blockImpl{entries: entries}, nil
}

var _ Block = (*blockImpl)(nil)

// Get performs binary search to find the entry for the given key.
func (b *blockImpl) Get(key []byte) (*common.Entry, bool) {
	left, right := 0, len(b.entries)
	for left < right {
		mid := (left + right) / 2
		cmp := bytes.Compare(key, b.entries[mid].Key)
		if cmp == 0 {
			return b.entries[mid], true
		} else if cmp < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return nil, false
}

// Len returns the number of entries in this block.
func (b *blockImpl) Len() int {
	return len(b.entries)
}
