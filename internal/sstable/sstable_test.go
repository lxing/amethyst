package sstable

import (
	"bytes"
	"os"
	"testing"

	"amethyst/internal/block"
	"amethyst/internal/common"
	"github.com/stretchr/testify/require"
)

// testIterator is a simple iterator for testing
type testIterator struct {
	entries []*common.Entry
	index   int
}

func (it *testIterator) Next() (*common.Entry, error) {
	if it.index >= len(it.entries) {
		return nil, nil
	}
	entry := it.entries[it.index]
	it.index++
	return entry, nil
}

func TestWriteSSTable(t *testing.T) {
	// Create test entries (3 entries - less than BLOCK_SIZE)
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("apple"), Value: []byte("red")},
		{Type: common.EntryTypePut, Seq: 2, Key: []byte("banana"), Value: []byte("yellow")},
		{Type: common.EntryTypePut, Seq: 3, Key: []byte("cherry"), Value: []byte("red")},
	}

	iter := &testIterator{entries: entries}
	var buf bytes.Buffer

	// Write SSTable
	totalBytes, err := WriteSSTable(&buf, iter)
	require.NoError(t, err)
	require.Greater(t, totalBytes, uint64(0))
	require.Equal(t, totalBytes, uint64(buf.Len()))

	// Read and verify footer (last FOOTER_SIZE bytes)
	data := buf.Bytes()
	footerData := data[len(data)-FOOTER_SIZE:]
	footer, err := ReadFooter(bytes.NewReader(footerData))
	require.NoError(t, err)
	require.NotNil(t, footer)

	// Verify footer offsets are valid
	require.Greater(t, footer.IndexOffset, uint64(0))
	require.LessOrEqual(t, footer.IndexOffset, uint64(len(data)-FOOTER_SIZE))

	// Read and verify index
	indexData := data[footer.IndexOffset : len(data)-FOOTER_SIZE]
	index, err := ReadIndex(bytes.NewReader(indexData))
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Equal(t, 1, len(index.Entries)) // Should have 1 block (3 entries < BLOCK_SIZE)
	require.Equal(t, uint64(0), index.Entries[0].BlockOffset)
	require.Equal(t, []byte("apple"), index.Entries[0].Key)
}

func TestSSTableReaderBasic(t *testing.T) {
	// Create test entries (3 entries - less than BLOCK_SIZE)
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("apple"), Value: []byte("red")},
		{Type: common.EntryTypePut, Seq: 2, Key: []byte("banana"), Value: []byte("yellow")},
		{Type: common.EntryTypePut, Seq: 3, Key: []byte("cherry"), Value: []byte("red")},
	}

	// Write SSTable to temp file
	tmpFile := t.TempDir() + "/test.sst"
	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	iter := &testIterator{entries: entries}
	_, err = WriteSSTable(f, iter)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open SSTable for reading
	reader, err := OpenSSTable(tmpFile, common.FileNo(1), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Test exact matches
	for _, expected := range entries {
		entry, found, err := reader.Get(expected.Key)
		require.NoError(t, err)
		require.True(t, found, "key %s should be found", expected.Key)
		require.NotNil(t, entry)
		require.Equal(t, expected.Type, entry.Type)
		require.Equal(t, expected.Seq, entry.Seq)
		require.Equal(t, expected.Key, entry.Key)
		require.Equal(t, expected.Value, entry.Value)
	}

	// Test keys that don't exist
	testCases := []struct {
		name string
		key  string
	}{
		{"Before first", "aaa"},
		{"Between apple and banana", "apricot"},
		{"Between banana and cherry", "blueberry"},
		{"After last", "durian"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entry, found, err := reader.Get([]byte(tc.key))
			require.NoError(t, err)
			require.False(t, found, "key %s should not be found", tc.key)
			require.Nil(t, entry)
		})
	}
}

func TestSSTableReaderMultipleBlocks(t *testing.T) {
	// Create enough entries to span multiple blocks
	numEntries := block.BLOCK_SIZE*2 + 10
	entries := make([]*common.Entry, numEntries)
	for i := 0; i < numEntries; i++ {
		key := []byte{byte(i / 256), byte(i % 256)} // 2-byte key
		entries[i] = &common.Entry{
			Type:  common.EntryTypePut,
			Seq:   uint64(i + 1),
			Key:   key,
			Value: []byte{byte(i)},
		}
	}

	// Write SSTable to temp file
	tmpFile := t.TempDir() + "/test_multi.sst"
	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	iter := &testIterator{entries: entries}
	_, err = WriteSSTable(f, iter)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open SSTable for reading
	reader, err := OpenSSTable(tmpFile, common.FileNo(1), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Verify reader has multiple blocks in index
	require.Greater(t, len(reader.index.Entries), 1, "should have multiple blocks")

	// Test reading from different blocks
	testIndices := []int{0, block.BLOCK_SIZE / 2, block.BLOCK_SIZE, block.BLOCK_SIZE + 50, numEntries - 1}
	for _, idx := range testIndices {
		expected := entries[idx]
		entry, found, err := reader.Get(expected.Key)
		require.NoError(t, err, "reading entry at index %d", idx)
		require.True(t, found, "entry at index %d should be found", idx)
		require.NotNil(t, entry)
		require.Equal(t, expected.Seq, entry.Seq)
		require.Equal(t, expected.Key, entry.Key)
		require.Equal(t, expected.Value, entry.Value)
	}
}

func TestSSTableReaderTombstone(t *testing.T) {
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("active"), Value: []byte("value")},
		{Type: common.EntryTypeDelete, Seq: 2, Key: []byte("deleted"), Value: nil},
	}

	// Write SSTable to temp file
	tmpFile := t.TempDir() + "/test_tombstone.sst"
	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	iter := &testIterator{entries: entries}
	_, err = WriteSSTable(f, iter)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open SSTable for reading
	reader, err := OpenSSTable(tmpFile, common.FileNo(1), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Verify tombstone is found
	entry, found, err := reader.Get([]byte("deleted"))
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, entry)
	require.Equal(t, common.EntryTypeDelete, entry.Type)
	require.Equal(t, uint64(2), entry.Seq)
}
