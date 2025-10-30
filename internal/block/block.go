package block

import (
	"bytes"
	"encoding/binary"
	"io"

	"amethyst/internal/common"
)

// Block Layout:
//
// A block is the smallest unit of storage in an SSTable, containing
// a fixed number of sorted entries with an offset footer for efficient
// binary search with lazy parsing.
//
//  offset 0 ────> ┌─────────────────┐
//                 │     Entry 0     │  sorted by key
//  offset X ────> ├─────────────────┤
//                 │     Entry 1     │
//  offset Y ────> ├─────────────────┤
//                 │     Entry 2     │
//                 ├─────────────────┤
//                 │       ...       │
//                 ├─────────────────┤
//                 │    offset_0     │  uint16 - byte offset to entry 0
//                 ├─────────────────┤
//                 │    offset_1     │  uint16
//                 ├─────────────────┤
//                 │    offset_2     │  uint16
//                 ├─────────────────┤
//                 │       ...       │
//                 ├─────────────────┤
//                 │    offset_N     │  uint16
//                 ├─────────────────┤
//                 │  num_entries    │  uint16 - count of entries
//                 └─────────────────┘
//
// The offset footer enables O(log n) binary search:
// 1. Binary search on offsets array
// 2. Jump to entry at offsets[mid]
// 3. Read just the key using common.ReadKey()
// 4. Compare and adjust search bounds
// 5. On match: parse full entry using common.ReadEntry()
//
// Memory layout:
// - data: contains only the entry data section (offsets excluded)
// - offsets: parsed from footer, stored as separate array
//
// This design enables lazy parsing - only entries accessed during
// binary search are decoded, significantly reducing memory usage
// and improving block loading performance.

// blockImpl stores raw entry bytes and offsets for lazy parsing.
type blockImpl struct {
	data    []byte   // Raw entry data section
	offsets []uint16 // Byte offset to start of each entry
}

var _ Block = (*blockImpl)(nil)

// NewBlock parses a raw data block with offset footer.
// Block format: [entry_data][offset0]...[offsetN][num_entries(u16)]
func NewBlock(data []byte) (Block, error) {
	if len(data) < 2 {
		return nil, io.ErrUnexpectedEOF
	}

	// Read num_entries from last 2 bytes
	numEntries := binary.LittleEndian.Uint16(data[len(data)-2:])

	// Calculate where offsets start
	offsetsStart := len(data) - 2 - int(numEntries)*2
	if offsetsStart < 0 {
		return nil, io.ErrUnexpectedEOF
	}

	// Parse offsets array
	offsets := make([]uint16, numEntries)
	for i := 0; i < int(numEntries); i++ {
		pos := offsetsStart + i*2
		offsets[i] = binary.LittleEndian.Uint16(data[pos : pos+2])
	}

	// Data section is everything before offsets
	dataSection := data[:offsetsStart]

	return &blockImpl{
		data:    dataSection,
		offsets: offsets,
	}, nil
}

// Get performs binary search on offsets with lazy entry parsing.
func (b *blockImpl) Get(key []byte) (*common.Entry, bool) {
	left, right := 0, len(b.offsets)

	for left < right {
		mid := (left + right) / 2

		// Jump to entry at offsets[mid]
		offset := b.offsets[mid]
		reader := bytes.NewReader(b.data[offset:])

		// Read just the key for comparison
		entryKey, err := common.ReadKey(reader)
		if err != nil {
			return nil, false
		}

		// Compare keys
		cmp := bytes.Compare(key, entryKey)
		if cmp == 0 {
			// Found! Parse the full entry from the beginning
			reader := bytes.NewReader(b.data[offset:])
			entry, err := common.ReadEntry(reader)
			if err != nil {
				return nil, false
			}
			return entry, true
		} else if cmp < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return nil, false
}

// Len returns the number of entries in this block.
func (b *blockImpl) Len() int {
	return len(b.offsets)
}
