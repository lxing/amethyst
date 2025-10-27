package block

import (
	"bytes"
	"fmt"
	"testing"

	"amethyst/internal/common"

	"github.com/stretchr/testify/require"
)

// testBlockWithEntries creates a block with n entries and verifies lookups.
func testBlockWithEntries(t *testing.T, n int) {
	require.True(t, n <= BLOCK_SIZE, "test requires n <= BLOCK_SIZE")

	// Create n sorted entries (using letters for keys)
	entries := make([]*common.Entry, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%02d", i)
		entries[i] = &common.Entry{
			Type:  common.EntryTypePut,
			Seq:   uint64(i + 1),
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("value_%02d", i)),
		}
	}

	// Encode all entries into a block
	var buf bytes.Buffer
	for _, e := range entries {
		_, err := common.WriteEntry(&buf, e)
		require.NoError(t, err)
	}

	// Parse the block
	block, err := NewBlock(buf.Bytes())
	require.NoError(t, err)

	// Verify all entries can be found
	for i, expected := range entries {
		found, ok := block.Get(expected.Key)
		require.True(t, ok, "key %d should be found", i)
		require.NotNil(t, found, "key %d should be found", i)
		require.Equal(t, expected.Type, found.Type)
		require.Equal(t, expected.Seq, found.Seq)
		require.Equal(t, expected.Key, found.Key)
		require.Equal(t, expected.Value, found.Value)
	}

	// Verify negative cases (keys not in block)
	negatives := []string{
		"aaa",           // before all keys
		"key_00_extra",  // between keys
		"key_99",        // after all keys
		"missing",       // arbitrary missing key
		"",              // empty key
	}

	for _, neg := range negatives {
		found, ok := block.Get([]byte(neg))
		require.False(t, ok, "key %s should not be found", neg)
		require.Nil(t, found, "key %s should not be found", neg)
	}
}

func TestBlockFullSize(t *testing.T) {
	// Test with a full block (BLOCK_SIZE entries)
	testBlockWithEntries(t, BLOCK_SIZE)
}

func TestBlockPartialSize(t *testing.T) {
	// Test with a partial block (fewer than BLOCK_SIZE entries)
	// Simulates the last block in an SSTable
	testBlockWithEntries(t, BLOCK_SIZE-3)
}

func TestBlockEmpty(t *testing.T) {
	block, err := NewBlock([]byte{})
	require.NoError(t, err)

	found, ok := block.Get([]byte("any"))
	require.False(t, ok)
	require.Nil(t, found)
}

func TestBlockWithTombstone(t *testing.T) {
	// Create a block with a tombstone entry
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("active"), Value: []byte("value")},
		{Type: common.EntryTypeDelete, Seq: 2, Key: []byte("deleted"), Value: nil},
	}

	var buf bytes.Buffer
	for _, e := range entries {
		_, err := common.WriteEntry(&buf, e)
		require.NoError(t, err)
	}

	block, err := NewBlock(buf.Bytes())
	require.NoError(t, err)

	// Verify tombstone is found
	found, ok := block.Get([]byte("deleted"))
	require.True(t, ok)
	require.NotNil(t, found)
	require.Equal(t, common.EntryTypeDelete, found.Type)
	require.Equal(t, uint64(2), found.Seq)
}
