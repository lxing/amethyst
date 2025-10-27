package sstable

import (
	"bytes"
	"io"

	"amethyst/internal/common"
)

// Index Block Layout:
//
// ┌──────────────────┐
// │    numEntries    │  uint64 - number of data blocks
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
// │      keyLen      │  uint64
// ├──────────────────┤
// │       key        │  []byte
// └──────────────────┘

// IndexEntry represents a single entry in the index block.
type IndexEntry struct {
	BlockOffset uint64 // File offset where data block starts
	Key         []byte // First key in the data block
}

// Encode writes an index entry to the given writer.
// Returns the number of bytes written.
func (e *IndexEntry) Encode(w io.Writer) (int, error) {
	total := 0

	n, err := common.WriteUint64(w, e.BlockOffset)
	total += n
	if err != nil {
		return total, err
	}

	n, err = common.WriteUint64(w, uint64(len(e.Key)))
	total += n
	if err != nil {
		return total, err
	}

	if len(e.Key) > 0 {
		n, err = common.WriteBytes(w, e.Key)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// DecodeIndexEntry reads a single index entry from the reader.
func DecodeIndexEntry(r io.Reader) (*IndexEntry, error) {
	blockOffset, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	keyLen, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	key, err := common.ReadBytes(r, keyLen)
	if err != nil {
		return nil, err
	}
	return &IndexEntry{
		BlockOffset: blockOffset,
		Key:         key,
	}, nil
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

// WriteIndex writes the entire index block to a writer.
// Returns the number of bytes written.
func WriteIndex(w io.Writer, idx *Index) (int, error) {
	total := 0

	n, err := common.WriteUint64(w, uint64(len(idx.Entries)))
	total += n
	if err != nil {
		return total, err
	}

	for i := range idx.Entries {
		n, err = idx.Entries[i].Encode(w)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// ReadIndex reads an entire index block from a reader.
func ReadIndex(r io.Reader) (*Index, error) {
	numEntries, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}
	entries := make([]IndexEntry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		entry, err := DecodeIndexEntry(r)
		if err != nil {
			return nil, err
		}
		entries[i] = *entry
	}
	return &Index{Entries: entries}, nil
}
