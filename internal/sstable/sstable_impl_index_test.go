package sstable

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexEntryEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		entry *IndexEntry
	}{
		{
			name: "Basic entry",
			entry: &IndexEntry{
				BlockOffset: 1024,
				Key:         []byte("apple"),
			},
		},
		{
			name: "Zero offset",
			entry: &IndexEntry{
				BlockOffset: 0,
				Key:         []byte("first-key"),
			},
		},
		{
			name: "Large offset",
			entry: &IndexEntry{
				BlockOffset: 0xFFFFFFFFFFFFFFFF,
				Key:         []byte("last-key"),
			},
		},
		{
			name: "Empty key",
			entry: &IndexEntry{
				BlockOffset: 500,
				Key:         nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			err := tt.entry.Encode(&buf)
			require.NoError(t, err)

			// Decode
			decoded, err := DecodeIndexEntry(&buf)
			require.NoError(t, err)
			require.NotNil(t, decoded)

			// Verify
			require.Equal(t, tt.entry.BlockOffset, decoded.BlockOffset)
			require.Equal(t, tt.entry.Key, decoded.Key)
		})
	}
}

func TestIndexFindBlockOffset(t *testing.T) {
	idx := &Index{
		Entries: []IndexEntry{
			{BlockOffset: 0, Key: []byte("apple")},
			{BlockOffset: 1000, Key: []byte("banana")},
			{BlockOffset: 2000, Key: []byte("cherry")},
			{BlockOffset: 3000, Key: []byte("durian")},
			{BlockOffset: 4000, Key: []byte("elderberry")},
		},
	}

	tests := []struct {
		name        string
		key         string
		wantOffset  uint64
		wantFound   bool
	}{
		{
			name:       "Before apple",
			key:        "aardvark",
			wantOffset: 0,
			wantFound:  false,
		},
		{
			name:       "Exact match apple",
			key:        "apple",
			wantOffset: 0,
			wantFound:  true,
		},
		{
			name:       "Between apple and banana",
			key:        "apricot",
			wantOffset: 0,
			wantFound:  true,
		},
		{
			name:       "Exact match banana",
			key:        "banana",
			wantOffset: 1000,
			wantFound:  true,
		},
		{
			name:       "Between banana and cherry",
			key:        "blueberry",
			wantOffset: 1000,
			wantFound:  true,
		},
		{
			name:       "Exact match cherry",
			key:        "cherry",
			wantOffset: 2000,
			wantFound:  true,
		},
		{
			name:       "Between cherry and durian",
			key:        "cranberry",
			wantOffset: 2000,
			wantFound:  true,
		},
		{
			name:       "Exact match durian",
			key:        "durian",
			wantOffset: 3000,
			wantFound:  true,
		},
		{
			name:       "Between durian and elderberry",
			key:        "eggplant",
			wantOffset: 3000,
			wantFound:  true,
		},
		{
			name:       "Exact match elderberry",
			key:        "elderberry",
			wantOffset: 4000,
			wantFound:  true,
		},
		{
			name:       "After elderberry",
			key:        "fig",
			wantOffset: 4000,
			wantFound:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, found := idx.FindBlockOffset([]byte(tt.key))
			require.Equal(t, tt.wantFound, found)
			if found {
				require.Equal(t, tt.wantOffset, offset)
			}
		})
	}
}

func TestIndexFindBlockOffset_EmptyIndex(t *testing.T) {
	idx := &Index{Entries: []IndexEntry{}}
	offset, found := idx.FindBlockOffset([]byte("any"))
	require.False(t, found)
	require.Equal(t, uint64(0), offset)
}

func TestIndexWriteRead(t *testing.T) {
	original := &Index{
		Entries: []IndexEntry{
			{BlockOffset: 0, Key: []byte("apple")},
			{BlockOffset: 1000, Key: []byte("banana")},
			{BlockOffset: 2000, Key: []byte("cherry")},
		},
	}

	// Write
	var buf bytes.Buffer
	err := WriteIndex(&buf, original)
	require.NoError(t, err)

	// Read
	decoded, err := ReadIndex(&buf)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	// Verify
	require.Equal(t, len(original.Entries), len(decoded.Entries))
	for i := range original.Entries {
		require.Equal(t, original.Entries[i].BlockOffset, decoded.Entries[i].BlockOffset)
		require.Equal(t, original.Entries[i].Key, decoded.Entries[i].Key)
	}
}

func TestIndexWriteRead_EmptyIndex(t *testing.T) {
	original := &Index{Entries: []IndexEntry{}}

	// Write
	var buf bytes.Buffer
	err := WriteIndex(&buf, original)
	require.NoError(t, err)

	// Read
	decoded, err := ReadIndex(&buf)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, 0, len(decoded.Entries))
}
