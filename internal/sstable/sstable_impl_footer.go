package sstable

import (
	"io"

	"amethyst/internal/common"
)

const (
	// FOOTER_SIZE is the size of the footer in bytes.
	// footerOffset = len(sstable) - FOOTER_SIZE
	FOOTER_SIZE = 16
)

// Footer is the last 16 bytes of the SSTable file.
type Footer struct {
	FilterOffset uint64 // Offset where filter block starts (8 bytes)
	IndexOffset  uint64 // Offset where index block starts (8 bytes)
}

// WriteFooter writes the footer to the given writer.
// Returns the number of bytes written.
func WriteFooter(w io.Writer, f *Footer) (int, error) {
	total := 0

	n, err := common.WriteUint64(w, f.FilterOffset)
	total += n
	if err != nil {
		return total, err
	}

	n, err = common.WriteUint64(w, f.IndexOffset)
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadFooter reads a footer from the reader.
func ReadFooter(r io.Reader) (*Footer, error) {
	filterOffset, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	indexOffset, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	return &Footer{
		FilterOffset: filterOffset,
		IndexOffset:  indexOffset,
	}, nil
}
