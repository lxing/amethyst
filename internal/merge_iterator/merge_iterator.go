package merge_iterator

import (
	"amethyst/internal/common"
)

// MergeIterator performs k-way merge of multiple sorted iterators.
// Used during compaction to merge multiple SSTables while preserving sort order.
type MergeIterator struct {
	iters []common.EntryIterator
	// TODO: Add min-heap for efficient k-way merge
	// TODO: Track current entry from each iterator
}

// NewMergeIterator creates a new merge iterator from multiple sorted iterators.
func NewMergeIterator(iters []common.EntryIterator) *MergeIterator {
	return &MergeIterator{
		iters: iters,
	}
}

// Next returns the next entry in sorted order across all iterators.
// TODO: Implement proper k-way merge using min-heap
// TODO: Handle sequence numbers (higher seq wins for same key)
// TODO: Handle tombstones (can drop if no older versions)
func (m *MergeIterator) Next() (*common.Entry, error) {
	// STUB: Just return nil for now
	// Real implementation will:
	// 1. Pop minimum entry from heap
	// 2. Advance that iterator
	// 3. Re-insert into heap
	// 4. Handle duplicates (keep newest by seq)
	// 5. Handle tombstones
	return nil, nil
}
