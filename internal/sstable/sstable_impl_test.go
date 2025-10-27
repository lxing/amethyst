package sstable

import (
	"bytes"
	"testing"

	"amethyst/internal/common"
	"github.com/stretchr/testify/require"
)

func TestFooterEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		footer Footer
	}{
		{
			name: "Basic footer",
			footer: Footer{
				FilterOffset: 1000,
				IndexOffset:  2000,
			},
		},
		{
			name: "Zero offsets",
			footer: Footer{
				FilterOffset: 0,
				IndexOffset:  0,
			},
		},
		{
			name: "Large offsets",
			footer: Footer{
				FilterOffset: 0xFFFFFFFFFFFFFFFF,
				IndexOffset:  0xFFFFFFFFFFFFFFFE,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			n, err := WriteFooter(&buf, &tt.footer)
			require.NoError(t, err)
			require.Equal(t, FOOTER_SIZE, n)
			require.Equal(t, FOOTER_SIZE, buf.Len())

			// Decode
			decoded, err := ReadFooter(&buf)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, tt.footer.FilterOffset, decoded.FilterOffset)
			require.Equal(t, tt.footer.IndexOffset, decoded.IndexOffset)
		})
	}
}

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
