package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"amethyst/internal/block"
	"amethyst/internal/common"
	"amethyst/internal/dsa/bloom"
	"amethyst/internal/dsa/lru"
)

var ErrNotFound = errors.New("key not found")

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
	BytesWritten uint32
	SmallestKey  []byte
	LargestKey   []byte
	EntryCount   uint32
}

// WriteSSTable writes a complete SSTable from a stream of sorted entries.
// w: writer to write SSTable data to
// entries: iterator providing sorted entries to write
// sizeHint: expected number of entries (for bloom filter sizing)
// fpr: bloom filter false positive rate (e.g., 0.01 for 1%)
// Returns metadata about the written SSTable to load into manifest.
func WriteSSTable(
	w io.Writer,
	entries common.EntryIterator,
	sizeHint uint32,
	fpr float64,
) (*WriteResult, error) {
	var offset uint32
	var indexEntries []IndexEntry
	var totalEntryCount uint32
	var smallestKey []byte
	var largestKeyRef []byte

	// Create bloom filter
	k, m := bloom.OptimalBloomFilterParams(sizeHint, fpr)
	bloomFilter := bloom.NewBloomFilter(k, m)

	// Block building state
	var blockOffsets []uint16
	var firstBlockKey []byte
	var blockStartOffset uint32

	// Flush current block: write offset footer and add to index
	flushBlock := func() error {
		if len(blockOffsets) == 0 {
			return nil
		}

		// Write offset footer
		for _, off := range blockOffsets {
			n, err := common.WriteUint16(w, off)
			if err != nil {
				return err
			}
			offset += uint32(n)
		}

		// Write num_entries
		n, err := common.WriteUint16(w, uint16(len(blockOffsets)))
		if err != nil {
			return err
		}
		offset += uint32(n)

		// Add index entry
		indexEntries = append(indexEntries, IndexEntry{
			BlockOffset: blockStartOffset,
			Key:         firstBlockKey,
		})

		// Reset for next block
		blockOffsets = nil
		firstBlockKey = nil

		return nil
	}

	// Stream entries and build blocks
	for {
		entry, err := entries.Next()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break // End of stream
		}

		if totalEntryCount == 0 {
			smallestKey = bytes.Clone(entry.Key)
		}
		largestKeyRef = entry.Key

		// Add to bloom filter
		bloomFilter.Add(entry.Key)

		// Flush block when full (before starting new one)
		if len(blockOffsets) >= block.BLOCK_SIZE {
			if err := flushBlock(); err != nil {
				return nil, err
			}
		}

		// Start new block if needed
		if len(blockOffsets) == 0 {
			blockStartOffset = offset
			firstBlockKey = bytes.Clone(entry.Key)
		}

		// Record offset relative to block start
		blockOffsets = append(blockOffsets, uint16(offset-blockStartOffset))

		// Write entry directly to output
		n, err := common.WriteEntry(w, entry)
		if err != nil {
			return nil, err
		}
		offset += uint32(n)
		totalEntryCount++
	}

	// Flush last partial block if any
	if err := flushBlock(); err != nil {
		return nil, err
	}

	// Clone largest key now that iteration is complete
	largestKey := bytes.Clone(largestKeyRef)

	// Write filter block
	filterOffset := offset
	n, err := bloom.WriteBloomFilter(w, bloomFilter)
	if err != nil {
		return nil, err
	}
	offset += uint32(n)

	// Write index block
	indexOffset := offset
	index := &Index{Entries: indexEntries}
	n, err = WriteIndex(w, index)
	if err != nil {
		return nil, err
	}
	offset += uint32(n)

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
	offset += uint32(n)

	return &WriteResult{
		BytesWritten: offset,
		SmallestKey:  smallestKey,
		LargestKey:   largestKey,
		EntryCount:   totalEntryCount,
	}, nil
}

type BlockKey struct {
	FileNo  common.FileNo
	BlockNo common.BlockNo
}

type SSTable struct {
	file       *os.File
	path       string
	fileNo     common.FileNo
	footer     *Footer
	filter     *bloom.BloomFilter
	index      *Index
	blockCache *lru.LRUCache[BlockKey, *block.Block]
}

// loadSSTableMetadata reads and parses the footer, filter, and index from an open SSTable file.
func loadSSTableMetadata(f *os.File) (*Footer, *bloom.BloomFilter, *Index, error) {
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

	// Read filter block
	filterSize := int64(footer.IndexOffset) - int64(footer.FilterOffset)
	var bloomFilter *bloom.BloomFilter
	if filterSize > 0 {
		filterData := make([]byte, filterSize)
		if _, err := f.ReadAt(filterData, int64(footer.FilterOffset)); err != nil {
			return nil, nil, nil, err
		}
		bloomFilter, err = bloom.ReadBloomFilter(bytes.NewReader(filterData))
		if err != nil {
			return nil, nil, nil, err
		}
	}

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

func OpenSSTable(
	path string,
	fileNo common.FileNo,
	blockCache *lru.LRUCache[BlockKey, *block.Block],
) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	footer, filter, index, err := loadSSTableMetadata(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to load metadata from %s: %w", path, err)
	}

	return &SSTable{
		file:       f,
		path:       path,
		fileNo:     fileNo,
		footer:     footer,
		filter:     filter,
		index:      index,
		blockCache: blockCache,
	}, nil
}

// Get looks up the entry for the given key.
// Returns ErrNotFound if the key does not exist.
func (s *SSTable) Get(key []byte) (*common.Entry, error) {
	// Check bloom filter first to skip disk read if key definitely not present
	if s.filter != nil && !s.filter.MayContain(key) {
		common.Logf("    not in %d.sst (filter reject)\n", s.fileNo)
		return nil, ErrNotFound
	}

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
	var blk *block.Block
	blockNo := common.BlockNo(blockIdx)
	cacheKey := BlockKey{FileNo: s.fileNo, BlockNo: blockNo}

	if s.blockCache != nil {
		if cachedBlock, ok := s.blockCache.Get(cacheKey); ok {
			blk = cachedBlock
		}
	}

	// Cache miss or no cache - read from disk
	if blk == nil {
		// Determine block size (read until next block or filter block)
		var blockEnd uint32
		if blockIdx+1 < len(s.index.Entries) {
			blockEnd = s.index.Entries[blockIdx+1].BlockOffset
		} else {
			blockEnd = s.footer.FilterOffset
		}

		blockSize := blockEnd - blockOffset
		blockData := make([]byte, blockSize)
		if _, err := s.file.ReadAt(blockData, int64(blockOffset)); err != nil {
			return nil, fmt.Errorf("failed to read block %d at offset %d from %s: %w", blockIdx, blockOffset, s.path, err)
		}

		// Parse block
		var err error
		blk, err = block.NewBlock(blockData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse block %d from %s: %w", blockIdx, s.path, err)
		}

		// Cache the parsed block if cache is available
		if s.blockCache != nil {
			s.blockCache.Put(cacheKey, blk)
		}
	}

	// Search within the block
	entry, found := blk.Get(key)
	if !found {
		return nil, ErrNotFound
	}
	return entry, nil
}

func (s *SSTable) GetIndex() *Index {
	return s.index
}

// Len returns the total number of entries in the SSTable.
// This value is cached in the footer for fast lookup.
func (s *SSTable) Len() int {
	return int(s.footer.EntryCount)
}

// Close releases the underlying file handle.
func (s *SSTable) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *SSTable) Iterator() common.EntryIterator {
	// Open a separate file handle for iteration
	f, err := os.Open(s.path)
	if err != nil {
		// Return an iterator that immediately fails
		return &sstableIterator{err: err}
	}

	return &sstableIterator{
		file:         f,
		index:        s.index,
		filterOffset: s.footer.FilterOffset,
		blockIdx:     0,
		entryIdx:     0,
	}
}

type sstableIterator struct {
	file         *os.File
	index        *Index
	filterOffset uint32

	blockIdx  int      // Current block index
	blockData []byte   // Current block's entry data section
	offsets   []uint16 // Current block's offsets
	entryIdx  int      // Position within current block

	err error // Initialization or loading error
}

var _ common.EntryIterator = (*sstableIterator)(nil)

func (it *sstableIterator) Next() (*common.Entry, error) {
	// Check for initialization error
	if it.err != nil {
		return nil, it.err
	}

	if it.file == nil {
		return nil, nil // Already closed
	}

	// Load first block if needed
	if it.blockData == nil {
		if err := it.loadNextBlock(); err != nil {
			if err == io.EOF {
				it.Close()
				return nil, nil
			}
			it.Close()
			return nil, err
		}
	}

	// If current block exhausted, load next block
	if it.entryIdx >= len(it.offsets) {
		if err := it.loadNextBlock(); err != nil {
			if err == io.EOF {
				it.Close()
				return nil, nil
			}
			it.Close()
			return nil, err
		}
	}

	// Parse entry at current position
	offset := it.offsets[it.entryIdx]
	reader := bytes.NewReader(it.blockData[offset:])
	entry, err := common.ReadEntry(reader)
	if err != nil {
		it.Close()
		return nil, err
	}

	it.entryIdx++
	return entry, nil
}

func (it *sstableIterator) loadNextBlock() error {
	if it.blockIdx >= len(it.index.Entries) {
		return io.EOF // Done
	}

	// Calculate block boundaries
	blockStart := it.index.Entries[it.blockIdx].BlockOffset
	var blockEnd uint32
	if it.blockIdx+1 < len(it.index.Entries) {
		blockEnd = it.index.Entries[it.blockIdx+1].BlockOffset
	} else {
		blockEnd = it.filterOffset
	}

	blockSize := blockEnd - blockStart
	blockBytes := make([]byte, blockSize)

	// Read full block
	_, err := it.file.ReadAt(blockBytes, int64(blockStart))
	if err != nil {
		return err
	}

	// Parse block: extract data section and offsets
	if len(blockBytes) < 2 {
		return io.ErrUnexpectedEOF
	}

	numEntries := binary.LittleEndian.Uint16(blockBytes[len(blockBytes)-2:])
	offsetsStart := len(blockBytes) - 2 - int(numEntries)*2

	if offsetsStart < 0 {
		return io.ErrUnexpectedEOF
	}

	// Extract offsets
	it.offsets = make([]uint16, numEntries)
	for i := 0; i < int(numEntries); i++ {
		pos := offsetsStart + i*2
		it.offsets[i] = binary.LittleEndian.Uint16(blockBytes[pos : pos+2])
	}

	// Extract entry data
	it.blockData = blockBytes[:offsetsStart]
	it.entryIdx = 0
	it.blockIdx++

	return nil
}

func (it *sstableIterator) Close() error {
	if it.file == nil {
		return nil
	}
	err := it.file.Close()
	it.file = nil
	it.blockData = nil
	it.offsets = nil
	return err
}
