package merge_iterator

import (
	"testing"

	"amethyst/internal/common"
	"github.com/stretchr/testify/require"
)

// sliceIterator is a test helper that wraps a slice as an EntryIterator
type sliceIterator struct {
	entries []*common.Entry
	index   int
}

func newSliceIterator(entries []*common.Entry) *sliceIterator {
	return &sliceIterator{entries: entries}
}

func (s *sliceIterator) Next() (*common.Entry, error) {
	if s.index >= len(s.entries) {
		return nil, nil
	}
	entry := s.entries[s.index]
	s.index++
	return entry, nil
}

func TestMergeIterator_ManyIterators(t *testing.T) {
	// Test with 5 iterators with both interleaved and duplicate keys
	// Duplicate keys should select highest sequence number
	iters := []common.EntryIterator{
		newSliceIterator([]*common.Entry{
			{Type: common.EntryTypePut, Seq: 1, Key: []byte("a"), Value: []byte("a1")},
			{Type: common.EntryTypePut, Seq: 2, Key: []byte("b"), Value: []byte("b2-old")}, // Will be overridden by seq=6
			{Type: common.EntryTypePut, Seq: 7, Key: []byte("f"), Value: []byte("f7")},
		}),
		newSliceIterator([]*common.Entry{
			{Type: common.EntryTypePut, Seq: 6, Key: []byte("b"), Value: []byte("b6-new")}, // Highest seq for "b"
			{Type: common.EntryTypePut, Seq: 8, Key: []byte("g"), Value: []byte("g8")},
		}),
		newSliceIterator([]*common.Entry{
			{Type: common.EntryTypePut, Seq: 3, Key: []byte("c"), Value: []byte("c3")},
			{Type: common.EntryTypePut, Seq: 9, Key: []byte("h"), Value: []byte("h9-old")}, // Will be overridden by seq=12
		}),
		newSliceIterator([]*common.Entry{
			{Type: common.EntryTypePut, Seq: 4, Key: []byte("d"), Value: []byte("d4")},
			{Type: common.EntryTypePut, Seq: 10, Key: []byte("i"), Value: []byte("i10")},
		}),
		newSliceIterator([]*common.Entry{
			{Type: common.EntryTypePut, Seq: 5, Key: []byte("e"), Value: []byte("e5")},
			{Type: common.EntryTypeDelete, Seq: 12, Key: []byte("h"), Value: nil}, // Highest seq for "h" - tombstone
			{Type: common.EntryTypePut, Seq: 11, Key: []byte("j"), Value: []byte("j11")},
		}),
	}

	m := NewMergeIterator(iters)

	expected := []struct {
		key   string
		value string
		seq   uint32
		typ   common.EntryType
	}{
		{"a", "a1", 1, common.EntryTypePut},
		{"b", "b6-new", 6, common.EntryTypePut},    // Higher seq wins
		{"c", "c3", 3, common.EntryTypePut},
		{"d", "d4", 4, common.EntryTypePut},
		{"e", "e5", 5, common.EntryTypePut},
		{"f", "f7", 7, common.EntryTypePut},
		{"g", "g8", 8, common.EntryTypePut},
		{"h", "", 12, common.EntryTypeDelete},      // Higher seq tombstone wins
		{"i", "i10", 10, common.EntryTypePut},
		{"j", "j11", 11, common.EntryTypePut},
	}

	for _, exp := range expected {
		got, err := m.Next()
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, exp.key, string(got.Key))
		require.Equal(t, exp.seq, got.Seq)
		require.Equal(t, exp.typ, got.Type)
		if exp.typ == common.EntryTypePut {
			require.Equal(t, exp.value, string(got.Value))
		}
	}

	// Should be exhausted
	got, err := m.Next()
	require.NoError(t, err)
	require.Nil(t, got)
}
