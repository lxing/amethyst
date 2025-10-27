package sstable

import (
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
// Returns the number of bytes written.
func (f *Footer) Encode(w io.Writer) (int, error) {
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

// DecodeFooter reads a footer from the reader.
func DecodeFooter(r io.Reader) (*Footer, error) {
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
	n, err := WriteIndex(w, index)
	if err != nil {
		return 0, err
	}
	offset += uint64(n)

	// Write footer
	footer := &Footer{
		FilterOffset: filterOffset,
		IndexOffset:  indexOffset,
	}
	n, err = footer.Encode(w)
	if err != nil {
		return 0, err
	}
	offset += uint64(n)

	return offset, nil
}
