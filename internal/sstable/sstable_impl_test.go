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
