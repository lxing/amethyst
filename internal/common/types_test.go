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
			n, err := tt.entry.Encode(&buf)
			require.NoError(t, err)
			require.Equal(t, n, buf.Len(), "returned byte count should match buffer size")

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
	// Empty buffer should return (nil, nil)
	var buf bytes.Buffer
	entry, err := DecodeEntry(&buf)
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestDecodeEntryIncomplete(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Incomplete header",
			data: []byte{0x00, 0x01, 0x02}, // Only 3 bytes of 17-byte header
		},
		{
			name: "Missing key data",
			data: []byte{
				0x00,                   // type
				0x2A, 0, 0, 0, 0, 0, 0, 0, // seq
				0x05, 0, 0, 0, // keyLen = 5
				0x00, 0, 0, 0, // valueLen = 0
				0x01, 0x02, // Only 2 of 5 key bytes
			},
		},
		{
			name: "Missing value data",
			data: []byte{
				0x00,                   // type
				0x2A, 0, 0, 0, 0, 0, 0, 0, // seq
				0x03, 0, 0, 0, // keyLen = 3
				0x05, 0, 0, 0, // valueLen = 5
				0x61, 0x62, 0x63, // key: "abc"
				0x01, 0x02, // Only 2 of 5 value bytes
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.data)
			entry, err := DecodeEntry(buf)
			require.ErrorIs(t, err, ErrIncompleteEntry)
			require.Nil(t, entry)
		})
	}
}
