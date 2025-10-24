package block

import (
	"bytes"
	"testing"

	"amethyst/internal/common"

	"github.com/stretchr/testify/require"
)

func TestBlockGetSingleEntry(t *testing.T) {
	// Create a block with a single entry
	entry := &common.Entry{
		Type:  common.EntryTypePut,
		Seq:   1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	var buf bytes.Buffer
	require.NoError(t, entry.Encode(&buf))

	block, err := NewBlock(buf.Bytes())
	require.NoError(t, err)

	// Get should find the entry
	found, err := block.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, common.EntryTypePut, found.Type)
	require.Equal(t, uint64(1), found.Seq)
	require.Equal(t, []byte("key1"), found.Key)
	require.Equal(t, []byte("value1"), found.Value)

	// Get should return nil for missing key
	notFound, err := block.Get([]byte("missing"))
	require.NoError(t, err)
	require.Nil(t, notFound)
}

func TestBlockGetMultipleEntries(t *testing.T) {
	// Create a block with multiple sorted entries
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("a"), Value: []byte("val_a")},
		{Type: common.EntryTypePut, Seq: 2, Key: []byte("c"), Value: []byte("val_c")},
		{Type: common.EntryTypePut, Seq: 3, Key: []byte("e"), Value: []byte("val_e")},
		{Type: common.EntryTypePut, Seq: 4, Key: []byte("g"), Value: []byte("val_g")},
	}

	var buf bytes.Buffer
	for _, e := range entries {
		require.NoError(t, e.Encode(&buf))
	}

	block, err := NewBlock(buf.Bytes())
	require.NoError(t, err)

	// Test finding each entry
	for _, expected := range entries {
		found, err := block.Get(expected.Key)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, expected.Type, found.Type)
		require.Equal(t, expected.Seq, found.Seq)
		require.Equal(t, expected.Key, found.Key)
		require.Equal(t, expected.Value, found.Value)
	}

	// Test missing keys
	for _, key := range []string{"b", "d", "f", "z"} {
		notFound, err := block.Get([]byte(key))
		require.NoError(t, err)
		require.Nil(t, notFound)
	}
}

func TestBlockGetWithTombstone(t *testing.T) {
	// Create a block with a tombstone entry
	entries := []*common.Entry{
		{Type: common.EntryTypePut, Seq: 1, Key: []byte("active"), Value: []byte("value")},
		{Type: common.EntryTypeDelete, Seq: 2, Key: []byte("deleted"), Value: nil},
	}

	var buf bytes.Buffer
	for _, e := range entries {
		require.NoError(t, e.Encode(&buf))
	}

	block, err := NewBlock(buf.Bytes())
	require.NoError(t, err)

	// Get should find the tombstone
	found, err := block.Get([]byte("deleted"))
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, common.EntryTypeDelete, found.Type)
	require.Equal(t, uint64(2), found.Seq)
}

func TestBlockGetEmptyBlock(t *testing.T) {
	block, err := NewBlock([]byte{})
	require.NoError(t, err)

	notFound, err := block.Get([]byte("any"))
	require.NoError(t, err)
	require.Nil(t, notFound)
}
