package common

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		entry *Entry
	}{
		{
			name: "Put entry with value",
			entry: &Entry{
				Type:  EntryTypePut,
				Seq:   42,
				Key:   []byte("test-key"),
				Value: []byte("test-value"),
			},
		},
		{
			name: "Delete entry (tombstone)",
			entry: &Entry{
				Type:  EntryTypeDelete,
				Seq:   100,
				Key:   []byte("deleted-key"),
				Value: nil,
			},
		},
		{
			name: "Nil key and value",
			entry: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   nil,
				Value: nil,
			},
		},
		{
			name: "Large value",
			entry: &Entry{
				Type:  EntryTypePut,
				Seq:   999,
				Key:   []byte("key"),
				Value: bytes.Repeat([]byte("x"), 1000),
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
			decoded, err := DecodeEntry(&buf)
			require.NoError(t, err)
			require.NotNil(t, decoded)

			// Verify
			require.Equal(t, tt.entry.Type, decoded.Type)
			require.Equal(t, tt.entry.Seq, decoded.Seq)
			require.Equal(t, tt.entry.Key, decoded.Key)
			require.Equal(t, tt.entry.Value, decoded.Value)
		})
	}
}

func TestDecodeEntryEOF(t *testing.T) {
	// Empty buffer should return EOF
	var buf bytes.Buffer
	entry, err := DecodeEntry(&buf)
	require.Error(t, err)
	require.Nil(t, entry)
}
