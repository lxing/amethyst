package sstable

import (
	"io"

	"amethyst/internal/common"
)

const (
	// FOOTER_SIZE is the size of the footer in bytes.
	// footerOffset = len(sstable) - FOOTER_SIZE
	FOOTER_SIZE = 12
)

// Footer is the last 12 bytes of the SSTable file.
type Footer struct {
	FilterOffset uint32 // Offset where filter block starts (4 bytes)
	IndexOffset  uint32 // Offset where index block starts (4 bytes)
	EntryCount   uint32 // Total number of entries in the SSTable (4 bytes)
}

// WriteFooter writes the footer to the given writer.
// Returns the number of bytes written.
func WriteFooter(w io.Writer, f *Footer) (int, error) {
	total := 0

	n, err := common.WriteUint32(w, f.FilterOffset)
	total += n
	if err != nil {
		return total, err
	}

	n, err = common.WriteUint32(w, f.IndexOffset)
	total += n
	if err != nil {
		return total, err
	}

	n, err = common.WriteUint32(w, f.EntryCount)
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadFooter reads a footer from the reader.
func ReadFooter(r io.Reader) (*Footer, error) {
	filterOffset, err := common.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	indexOffset, err := common.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	entryCount, err := common.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	return &Footer{
		FilterOffset: filterOffset,
		IndexOffset:  indexOffset,
		EntryCount:   entryCount,
	}, nil
}
