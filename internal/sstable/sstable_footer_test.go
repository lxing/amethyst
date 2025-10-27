package sstable

import (
	"bytes"
	"testing"

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
