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

// Index Block Layout:
//
// ┌──────────────────┐
// │    numEntries    │  uint32 - number of data blocks
// ├──────────────────┤
// │   IndexEntry 0   │
// ├──────────────────┤
// │   IndexEntry 1   │
// ├──────────────────┤
// │       ...        │
// ├──────────────────┤
// │  IndexEntry N-1  │
// └──────────────────┘
//
// IndexEntry Layout:
//
// ┌──────────────────┐
// │   blockOffset    │  uint64
// ├──────────────────┤
// │      keyLen      │  uint32
// ├──────────────────┤
// │       key        │  []byte
// └──────────────────┘

// IndexEntry represents a single entry in the index block.
type IndexEntry struct {
	BlockOffset uint64 // File offset where data block starts
	Key         []byte // First key in the data block
}

// Encode writes an index entry to the given writer.
func (e *IndexEntry) Encode(w io.Writer) error {
	var buf [8 + 4]byte

	binary.LittleEndian.PutUint64(buf[0:], e.BlockOffset)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(e.Key)))

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	if len(e.Key) > 0 {
		if _, err := w.Write(e.Key); err != nil {
			return err
		}
	}

	return nil
}

// DecodeIndexEntry reads a single index entry from the reader.
func DecodeIndexEntry(r io.Reader) (*IndexEntry, error) {
	var hdr [8 + 4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	entry := &IndexEntry{
		BlockOffset: binary.LittleEndian.Uint64(hdr[0:8]),
	}

	keyLen := binary.LittleEndian.Uint32(hdr[8:12])

	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r, entry.Key); err != nil {
			return nil, err
		}
	}

	return entry, nil
}
