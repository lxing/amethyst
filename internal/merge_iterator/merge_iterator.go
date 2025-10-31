package merge_iterator

import (
	"bytes"

	"amethyst/internal/common"
	"amethyst/internal/heap"
)

type MergeIterator struct {
	heap *heap.Heap[*common.Entry, common.EntryIterator]
}

func NewMergeIterator(iters []common.EntryIterator) *MergeIterator {
	h := heap.New[*common.Entry, common.EntryIterator](common.CompareEntries)

	for _, iter := range iters {
		entry, err := iter.Next()
		if err != nil {
			continue
		}
		if entry != nil {
			h.Push(entry, iter)
		}
	}

	return &MergeIterator{
		heap: h,
	}
}

func (m *MergeIterator) Next() (*common.Entry, error) {
	minEntry, ok := m.heap.Peek()
	if !ok {
		return nil, nil
	}

	for {
		_, iter, ok := m.heap.Pop()
		if !ok {
			break
		}

		nextEntry, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if nextEntry != nil {
			m.heap.Push(nextEntry, iter)
		}

		peekEntry, ok := m.heap.Peek()
		if !ok || !bytes.Equal(peekEntry.Key, minEntry.Key) {
			break
		}
	}

	return minEntry, nil
}
