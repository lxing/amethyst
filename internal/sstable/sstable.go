package sstable

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"

	"amethyst/internal/block"
	"amethyst/internal/block_cache"
	"amethyst/internal/common"
	"amethyst/internal/filter"
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

// WriteResult contains metadata from writing an SSTable.
type WriteResult struct {
	BytesWritten uint64
	SmallestKey  []byte
	LargestKey   []byte
	EntryCount   uint64
}

// WriteSSTable writes a complete SSTable from a stream of sorted entries.
// Returns metadata about the written SSTable.
func WriteSSTable(w io.Writer, entries common.EntryIterator) (*WriteResult, error) {
	var offset uint64
	var indexEntries []IndexEntry
	var blockEntryCount int
	var totalEntryCount uint64
	var blockStartOffset uint64
	var firstBlockKey []byte
	var smallestKey []byte
	var largestKey []byte

	// Stream data blocks
	for {
		entry, err := entries.Next()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break // End of stream
		}

		// Track smallest key (first entry)
		if totalEntryCount == 0 {
			smallestKey = bytes.Clone(entry.Key)
		}

		// Track largest key (last entry seen)
		largestKey = bytes.Clone(entry.Key)

		// Start new block: record offset and first key
		if blockEntryCount == 0 {
			blockStartOffset = offset
			firstBlockKey = make([]byte, len(entry.Key))
			copy(firstBlockKey, entry.Key)
		}

		// Write entry to output
		n, err := common.WriteEntry(w, entry)
		if err != nil {
			return nil, err
		}
		offset += uint64(n)
		blockEntryCount++
		totalEntryCount++

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
		return nil, err
	}
	offset += uint64(n)

	// Write footer
	footer := &Footer{
		FilterOffset: filterOffset,
		IndexOffset:  indexOffset,
		EntryCount:   totalEntryCount,
	}
	n, err = WriteFooter(w, footer)
	if err != nil {
		return nil, err
	}
	offset += uint64(n)

	return &WriteResult{
		BytesWritten: offset,
		SmallestKey:  smallestKey,
		LargestKey:   largestKey,
		EntryCount:   totalEntryCount,
	}, nil
}

// sstableImpl provides random access to entries in an SSTable file.
type sstableImpl struct {
	file       *os.File
	fileNo     common.FileNo
	footer     *Footer
	filter     filter.Filter
	index      *Index
	blockCache block_cache.BlockCache
}

var _ SSTable = (*sstableImpl)(nil)

// loadSSTableMetadata reads and parses the footer, filter, and index from an open SSTable file.
func loadSSTableMetadata(f *os.File) (*Footer, filter.Filter, *Index, error) {
	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return nil, nil, nil, err
	}
	fileSize := stat.Size()

	if fileSize < FOOTER_SIZE {
		return nil, nil, nil, io.ErrUnexpectedEOF
	}

	// Read footer from end of file
	footerOffset := fileSize - FOOTER_SIZE
	footerData := make([]byte, FOOTER_SIZE)
	if _, err := f.ReadAt(footerData, footerOffset); err != nil {
		return nil, nil, nil, err
	}

	footer, err := ReadFooter(bytes.NewReader(footerData))
	if err != nil {
		return nil, nil, nil, err
	}

	// TODO: Read filter block from footer.FilterOffset to footer.IndexOffset
	// For now, filter is unimplemented (just a placeholder offset in footer)
	var bloomFilter filter.Filter = nil

	// Read index block
	indexSize := footerOffset - int64(footer.IndexOffset)
	if indexSize <= 0 {
		return nil, nil, nil, io.ErrUnexpectedEOF
	}

	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, int64(footer.IndexOffset)); err != nil {
		return nil, nil, nil, err
	}

	index, err := ReadIndex(bytes.NewReader(indexData))
	if err != nil {
		return nil, nil, nil, err
	}

	return footer, bloomFilter, index, nil
}

// OpenSSTable opens an SSTable file and loads its footer and index into memory.
func OpenSSTable(
	path string,
	fileNo common.FileNo,
	blockCache block_cache.BlockCache,
) (*sstableImpl, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	footer, filter, index, err := loadSSTableMetadata(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to load metadata from %s: %w", path, err)
	}

	return &sstableImpl{
		file:       f,
		fileNo:     fileNo,
		footer:     footer,
		filter:     filter,
		index:      index,
		blockCache: blockCache,
	}, nil
}

// Get looks up the entry for the given key.
// Returns ErrNotFound if the key does not exist.
func (s *sstableImpl) Get(key []byte) (*common.Entry, error) {
	// Find which block might contain this key
	blockOffset, found := s.index.FindBlockOffset(key)
	if !found {
		return nil, ErrNotFound
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
		return nil, io.ErrUnexpectedEOF
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
			return nil, fmt.Errorf("failed to read block %d at offset %d from %s: %w", blockIdx, blockOffset, s.file.Name(), err)
		}

		// Parse block
		var err error
		blk, err = block.NewBlock(blockData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse block %d from %s: %w", blockIdx, s.file.Name(), err)
		}

		// Cache the parsed block if cache is available
		if s.blockCache != nil {
			s.blockCache.Put(s.fileNo, blockNo, blk)
		}
	}

	// Search within the block
	entry, found := blk.Get(key)
	if !found {
		return nil, ErrNotFound
	}
	return entry, nil
}

// GetIndex returns the index entries (first key of each block).
func (s *sstableImpl) GetIndex() *Index {
	return s.index
}

// Len returns the total number of entries in the SSTable.
// This value is cached in the footer for fast lookup.
func (s *sstableImpl) Len() int {
	return int(s.footer.EntryCount)
}

// Close releases the underlying file handle.
func (s *sstableImpl) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// Iterator returns an iterator that sequentially scans all entries in the SSTable.
func (s *sstableImpl) Iterator() common.EntryIterator {
	// Open a separate file handle for iteration
	f, err := os.Open(s.file.Name())
	if err != nil {
		// Return an iterator that immediately fails
		return &sstableIterator{err: err}
	}

	return &sstableIterator{
		file:   f,
		reader: bufio.NewReader(io.LimitReader(f, int64(s.footer.FilterOffset))),
	}
}

// sstableIterator provides sequential access to all entries in an SSTable.
type sstableIterator struct {
	file   *os.File
	reader *bufio.Reader
	err    error // Initialization error
}

var _ common.EntryIterator = (*sstableIterator)(nil)

// Next returns the next entry in the SSTable.
func (it *sstableIterator) Next() (*common.Entry, error) {
	// Check for initialization error
	if it.err != nil {
		return nil, it.err
	}

	if it.file == nil {
		return nil, nil // Already closed
	}

	// Read next entry sequentially
	entry, err := common.ReadEntry(it.reader)
	if err != nil {
		// EOF or read error
		it.Close()
		return nil, err
	}

	if entry == nil {
		// End of entries
		it.Close()
		return nil, nil
	}

	return entry, nil
}

// Close releases the underlying file handle.
func (it *sstableIterator) Close() error {
	if it.file == nil {
		return nil
	}
	err := it.file.Close()
	it.file = nil
	it.reader = nil
	return err
}
