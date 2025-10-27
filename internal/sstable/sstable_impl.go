package sstable

import (
	"bytes"
	"io"
	"os"

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
		n, err := common.WriteEntry(w, entry)
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
	n, err = WriteFooter(w, footer)
	if err != nil {
		return 0, err
	}
	offset += uint64(n)

	return offset, nil
}

// SSTableReader provides random access to entries in an SSTable file.
type SSTableReader struct {
	file   *os.File
	footer *Footer
	index  *Index
}

// OpenSSTable opens an SSTable file and loads its footer and index into memory.
func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < FOOTER_SIZE {
		f.Close()
		return nil, io.ErrUnexpectedEOF
	}

	// Read footer from end of file
	footerOffset := fileSize - FOOTER_SIZE
	footerData := make([]byte, FOOTER_SIZE)
	if _, err := f.ReadAt(footerData, footerOffset); err != nil {
		f.Close()
		return nil, err
	}

	footer, err := ReadFooter(bytes.NewReader(footerData))
	if err != nil {
		f.Close()
		return nil, err
	}

	// Read index block
	indexSize := footerOffset - int64(footer.IndexOffset)
	if indexSize <= 0 {
		f.Close()
		return nil, io.ErrUnexpectedEOF
	}

	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, int64(footer.IndexOffset)); err != nil {
		f.Close()
		return nil, err
	}

	index, err := ReadIndex(bytes.NewReader(indexData))
	if err != nil {
		f.Close()
		return nil, err
	}

	return &SSTableReader{
		file:   f,
		footer: footer,
		index:  index,
	}, nil
}

// Get looks up the entry for the given key.
// Returns (nil, false) if the key is not found.
func (r *SSTableReader) Get(key []byte) (*common.Entry, bool, error) {
	// Find which block might contain this key
	blockOffset, found := r.index.FindBlockOffset(key)
	if !found {
		return nil, false, nil
	}

	// Determine block size (read until next block or filter block)
	var blockEnd uint64
	blockIdx := -1
	for i, entry := range r.index.Entries {
		if entry.BlockOffset == blockOffset {
			blockIdx = i
			break
		}
	}

	if blockIdx == -1 {
		return nil, false, io.ErrUnexpectedEOF
	}

	// Block ends at the start of next block, or at filter offset if last block
	if blockIdx+1 < len(r.index.Entries) {
		blockEnd = r.index.Entries[blockIdx+1].BlockOffset
	} else {
		blockEnd = r.footer.FilterOffset
	}

	blockSize := blockEnd - blockOffset
	blockData := make([]byte, blockSize)
	if _, err := r.file.ReadAt(blockData, int64(blockOffset)); err != nil {
		return nil, false, err
	}

	// Parse block and search within it
	blk, err := block.NewBlock(blockData)
	if err != nil {
		return nil, false, err
	}

	entry, found := blk.Get(key)
	return entry, found, nil
}

// Close releases the underlying file handle.
func (r *SSTableReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}
