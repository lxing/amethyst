package sstable

import (
	"encoding/binary"
	"io"

	"amethyst/internal/block"
	"amethyst/internal/common"
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

// WriteSSTable writes a complete SSTable from a stream of sorted entries.
// Returns the total number of bytes written.
func WriteSSTable(w io.Writer, entries common.EntryIterator) (uint64, error) {
	var offset uint64
	var indexEntries []IndexEntry
	var blockEntryCount int
	var blockStartOffset uint64
	var firstBlockKey []byte

	// Stream data blocks
	for {
		entry, err := entries.Next()
		if err != nil {
			return 0, err
		}
		if entry == nil {
			break // End of stream
		}

		// Start new block: record offset and first key
		if blockEntryCount == 0 {
			blockStartOffset = offset
			firstBlockKey = make([]byte, len(entry.Key))
			copy(firstBlockKey, entry.Key)
		}

		// Write entry to output
		n, err := entry.Encode(w)
		if err != nil {
			return 0, err
		}
		offset += uint64(n)
		blockEntryCount++

		// Create index entry when block is full
		if blockEntryCount >= block.BLOCK_SIZE {
			indexEntry := IndexEntry{
				BlockOffset: blockStartOffset,
				Key:         firstBlockKey,
			}
			indexEntries = append(indexEntries, indexEntry)
			blockEntryCount = 0
			firstBlockKey = nil
		}
	}

	// Handle last partial block
	if blockEntryCount > 0 {
		indexEntry := IndexEntry{
			BlockOffset: blockStartOffset,
			Key:         firstBlockKey,
		}
		indexEntries = append(indexEntries, indexEntry)
	}

	// Write filter block (placeholder)
	filterOffset := offset
	// TODO: Implement bloom filter

	// Write index block
	indexOffset := offset
	index := &Index{Entries: indexEntries}
	if err := WriteIndex(w, index); err != nil {
		return 0, err
	}

	// Calculate index size
	indexSize := uint64(8) // numEntries
	for i := range indexEntries {
		indexSize += uint64(8 + 8 + len(indexEntries[i].Key))
	}
	offset += indexSize

	// Write footer
	footer := &Footer{
		FilterOffset: filterOffset,
		IndexOffset:  indexOffset,
	}
	if err := footer.Encode(w); err != nil {
		return 0, err
	}
	offset += FOOTER_SIZE

	return offset, nil
}
