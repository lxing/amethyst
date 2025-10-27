package sstable

import (
	"bytes"
	"io"
	"os"

	"amethyst/internal/block"
	"amethyst/internal/block_cache"
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

// SSTableImpl provides random access to entries in an SSTable file.
type SSTableImpl struct {
	file       *os.File
	fileNo     common.FileNo
	footer     *Footer
	index      *Index
	blockCache block_cache.BlockCache
}

// loadSSTableMetadata reads and parses the footer and index from an open SSTable file.
func loadSSTableMetadata(f *os.File) (*Footer, *Index, error) {
	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	fileSize := stat.Size()

	if fileSize < FOOTER_SIZE {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Read footer from end of file
	footerOffset := fileSize - FOOTER_SIZE
	footerData := make([]byte, FOOTER_SIZE)
	if _, err := f.ReadAt(footerData, footerOffset); err != nil {
		return nil, nil, err
	}

	footer, err := ReadFooter(bytes.NewReader(footerData))
	if err != nil {
		return nil, nil, err
	}

	// TODO: Read filter block from footer.FilterOffset to footer.IndexOffset
	// For now, filter is unimplemented (just a placeholder offset in footer)

	// Read index block
	indexSize := footerOffset - int64(footer.IndexOffset)
	if indexSize <= 0 {
		return nil, nil, io.ErrUnexpectedEOF
	}

	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, int64(footer.IndexOffset)); err != nil {
		return nil, nil, err
	}

	index, err := ReadIndex(bytes.NewReader(indexData))
	if err != nil {
		return nil, nil, err
	}

	return footer, index, nil
}

// OpenSSTable opens an SSTable file and loads its footer and index into memory.
func OpenSSTable(
	path string,
	fileNo common.FileNo,
	blockCache block_cache.BlockCache,
) (*SSTableImpl, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	footer, index, err := loadSSTableMetadata(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &SSTableImpl{
		file:       f,
		fileNo:     fileNo,
		footer:     footer,
		index:      index,
		blockCache: blockCache,
	}, nil
}

// Get looks up the entry for the given key.
// Returns (nil, false) if the key is not found.
func (s *SSTableImpl) Get(key []byte) (*common.Entry, bool, error) {
	// Find which block might contain this key
	blockOffset, found := s.index.FindBlockOffset(key)
	if !found {
		return nil, false, nil
	}

	// Find the block index in the index entries
	blockIdx := -1
	for i, entry := range s.index.Entries {
		if entry.BlockOffset == blockOffset {
			blockIdx = i
			break
		}
	}

	if blockIdx == -1 {
		return nil, false, io.ErrUnexpectedEOF
	}

	// Try to get block from cache
	var blk block.Block
	blockNo := common.BlockNo(blockIdx)

	if s.blockCache != nil {
		if cachedBlock, ok := s.blockCache.Get(s.fileNo, blockNo); ok {
			blk = cachedBlock
		}
	}

	// Cache miss or no cache - read from disk
	if blk == nil {
		// Determine block size (read until next block or filter block)
		var blockEnd uint64
		if blockIdx+1 < len(s.index.Entries) {
			blockEnd = s.index.Entries[blockIdx+1].BlockOffset
		} else {
			blockEnd = s.footer.FilterOffset
		}

		blockSize := blockEnd - blockOffset
		blockData := make([]byte, blockSize)
		if _, err := s.file.ReadAt(blockData, int64(blockOffset)); err != nil {
			return nil, false, err
		}

		// Parse block
		var err error
		blk, err = block.NewBlock(blockData)
		if err != nil {
			return nil, false, err
		}

		// Cache the parsed block if cache is available
		if s.blockCache != nil {
			s.blockCache.Put(s.fileNo, blockNo, blk)
		}
	}

	// Search within the block
	entry, found := blk.Get(key)
	return entry, found, nil
}

// Close releases the underlying file handle.
func (s *SSTableImpl) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
