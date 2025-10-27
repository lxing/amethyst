package sstable

import (
	"encoding/binary"
	"io"
)

// SSTable File Layout:
//
//                 ┌────────────────┐
//                 │  Data Block 0  │  block.BLOCK_SIZE entries, sorted by key (no duplicates)
//                 ├────────────────┤
//                 │  Data Block 1  │  block.BLOCK_SIZE entries
//                 ├────────────────┤
//                 │       ...      │
//                 ├────────────────┤
//                 │  Data Block N  │  up to block.BLOCK_SIZE entries
// filterOffset -> ├────────────────┤
//                 │  Filter Block  │  bloom filter
//  indexOffset -> ├────────────────┤
//                 │  Index Block   │  array of {firstKey, blockOffset} entries
// footerOffset -> ├────────────────┤
//                 │     Footer     │  footer: {filterOffset, indexOffset}
//                 └────────────────┘

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

// Encode writes the footer to the given writer.
func (f *Footer) Encode(w io.Writer) error {
	var buf [FOOTER_SIZE]byte
	binary.LittleEndian.PutUint64(buf[0:8], f.FilterOffset)
	binary.LittleEndian.PutUint64(buf[8:16], f.IndexOffset)
	_, err := w.Write(buf[:])
	return err
}

// DecodeFooter reads a footer from the reader.
func DecodeFooter(r io.Reader) (*Footer, error) {
	var buf [FOOTER_SIZE]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	return &Footer{
		FilterOffset: binary.LittleEndian.Uint64(buf[0:8]),
		IndexOffset:  binary.LittleEndian.Uint64(buf[8:16]),
	}, nil
}
