package merge_iterator

import (
	"bytes"

	"amethyst/internal/common"
)

// MergeIterator performs k-way merge of multiple sorted iterators.
// Used during compaction to merge multiple SSTables while preserving sort order.
// This is a naive O(k) implementation that scans all iterators per Next() call.
// TODO: Optimize with min-heap for O(log k) performance
type MergeIterator struct {
	iters   []common.EntryIterator
	current []*common.Entry // Current entry from each iterator (nil if exhausted)
}

// NewMergeIterator creates a new merge iterator from multiple sorted iterators.
func NewMergeIterator(iters []common.EntryIterator) *MergeIterator {
	m := &MergeIterator{
		iters:   iters,
		current: make([]*common.Entry, len(iters)),
	}

	// Initialize by reading first entry from each iterator
	for i, iter := range iters {
		entry, err := iter.Next()
		if err != nil {
			// If any iterator fails during initialization, we can't proceed
			// For now, treat as exhausted (nil entry)
			m.current[i] = nil
		} else {
			m.current[i] = entry
		}
	}

	return m
}

// Next returns the next entry in sorted order across all iterators.
// For duplicate keys (same key, different sequence numbers), returns the entry
// with the highest sequence number (newest version).
// Returns nil when all iterators are exhausted.
func (m *MergeIterator) Next() (*common.Entry, error) {
	// Find minimum entry among all current entries using CompareEntries
	// This handles both key ordering and sequence number selection
	var minEntry *common.Entry
	minIdx := -1

	for i, entry := range m.current {
		if entry == nil {
			continue // Iterator exhausted
		}

		if minEntry == nil || common.CompareEntries(entry, minEntry) < 0 {
			minEntry = entry
			minIdx = i
		}
	}

	// All iterators exhausted
	if minIdx == -1 {
		return nil, nil
	}

	// Collect all iterators that have the same key as minEntry
	// We need to advance all of them to skip duplicate keys
	var indicesToAdvance []int
	for i, entry := range m.current {
		if entry != nil && bytes.Equal(entry.Key, minEntry.Key) {
			indicesToAdvance = append(indicesToAdvance, i)
		}
	}

	// Advance all iterators that had this key
	for _, i := range indicesToAdvance {
		entry, err := m.iters[i].Next()
		if err != nil {
			return nil, err
		}
		m.current[i] = entry
	}

	return minEntry, nil
}
