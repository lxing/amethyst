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
			n, err := WriteEntry(&buf, tt.entry)
			require.NoError(t, err)
			require.Equal(t, n, buf.Len(), "returned byte count should match buffer size")

			// Decode
			decoded, err := ReadEntry(&buf)
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

func TestReadEntryEOF(t *testing.T) {
	// Empty buffer should return (nil, nil)
	var buf bytes.Buffer
	entry, err := ReadEntry(&buf)
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestReadEntryIncomplete(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Incomplete header",
			data: []byte{0x00, 0x01, 0x02}, // Only 3 bytes of 13-byte header
		},
		{
			name: "Missing key data",
			data: []byte{
				0x00,          // type
				0x2A, 0, 0, 0, // seq (uint32)
				0x05, 0, 0, 0, // keyLen = 5
				0x00, 0, 0, 0, // valueLen = 0
				0x01, 0x02, // Only 2 of 5 key bytes
			},
		},
		{
			name: "Missing value data",
			data: []byte{
				0x00,          // type
				0x2A, 0, 0, 0, // seq (uint32)
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
			entry, err := ReadEntry(buf)
			require.ErrorIs(t, err, ErrIncompleteEntry)
			require.Nil(t, entry)
		})
	}
}

func TestCompareEntries(t *testing.T) {
	tests := []struct {
		name     string
		e1       *Entry
		e2       *Entry
		expected int // negative if e1 < e2, 0 if equal, positive if e1 > e2
	}{
		{
			name: "Different keys - e1 < e2",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("a"),
				Value: []byte("1"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("b"),
				Value: []byte("2"),
			},
			expected: -1, // "a" < "b"
		},
		{
			name: "Different keys - e1 > e2",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("z"),
				Value: []byte("1"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("a"),
				Value: []byte("2"),
			},
			expected: 1, // "z" > "a"
		},
		{
			name: "Same key, same seq",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   5,
				Key:   []byte("key"),
				Value: []byte("value1"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   5,
				Key:   []byte("key"),
				Value: []byte("value2"),
			},
			expected: 0, // Equal
		},
		{
			name: "Same key, e1 has higher seq (e1 wins)",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   10,
				Key:   []byte("key"),
				Value: []byte("new"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   5,
				Key:   []byte("key"),
				Value: []byte("old"),
			},
			expected: -1, // Higher seq should be "less" (comes first)
		},
		{
			name: "Same key, e2 has higher seq (e2 wins)",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   3,
				Key:   []byte("key"),
				Value: []byte("old"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   8,
				Key:   []byte("key"),
				Value: []byte("new"),
			},
			expected: 1, // Lower seq should be "greater" (comes after)
		},
		{
			name: "Same key, different types, higher seq wins",
			e1: &Entry{
				Type:  EntryTypeDelete,
				Seq:   10,
				Key:   []byte("key"),
				Value: nil,
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   5,
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			expected: -1, // e1 has higher seq, so it wins
		},
		{
			name: "Lexicographic ordering - longer key",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("apple"),
				Value: []byte("1"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("app"),
				Value: []byte("2"),
			},
			expected: 1, // "apple" > "app"
		},
		{
			name: "Lexicographic ordering - prefix",
			e1: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("app"),
				Value: []byte("1"),
			},
			e2: &Entry{
				Type:  EntryTypePut,
				Seq:   1,
				Key:   []byte("apple"),
				Value: []byte("2"),
			},
			expected: -1, // "app" < "apple"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareEntries(tt.e1, tt.e2)

			// Check the sign of the result
			if tt.expected < 0 {
				require.Less(t, result, 0, "expected e1 < e2")
			} else if tt.expected > 0 {
				require.Greater(t, result, 0, "expected e1 > e2")
			} else {
				require.Equal(t, 0, result, "expected e1 == e2")
			}
		})
	}
}
