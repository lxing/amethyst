package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
)

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

// Index represents the in-memory parsed index block.
type Index struct {
	Entries []IndexEntry // Sorted by Key
}

// FindBlockOffset returns the block offset for the block that may contain the given key.
// Returns the offset of the block where entries[i].Key <= key < entries[i+1].Key.
// Returns (0, false) if the key is before the first block's first key.
func (idx *Index) FindBlockOffset(key []byte) (uint64, bool) {
	if len(idx.Entries) == 0 {
		return 0, false
	}

	// Check if key is before the first block
	if bytes.Compare(key, idx.Entries[0].Key) < 0 {
		return 0, false
	}

	// Binary search to find the largest entry where entry.Key <= key
	left, right := 0, len(idx.Entries)
	for left < right {
		mid := (left + right) / 2
		cmp := bytes.Compare(idx.Entries[mid].Key, key)
		if cmp <= 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// left is now the first entry with Key > key
	// We want the entry before it
	return idx.Entries[left-1].BlockOffset, true
}
