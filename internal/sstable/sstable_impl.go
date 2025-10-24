package sstable

// SSTable File Layout:
//
// ┌─────────────────┐
// │  Data Block 0   │  100 entries, sorted by key (no duplicates)
// ├─────────────────┤
// │  Data Block 1   │  100 entries
// ├─────────────────┤
// │      ...        │
// ├─────────────────┤
// │  Data Block N   │  100 entries (may have fewer in last block)
// ├─────────────────┤
// │ Filter Section  │  Bloom filter
// ├─────────────────┤
// │  Index Section  │  Array of {firstKey, blockOffset} entries
// ├─────────────────┤
// │     Footer      │  16 bytes: filterOffset + indexOffset
// └─────────────────┘

const (
	// BLOCK_SIZE is the target number of entries per data block.
	BLOCK_SIZE = 100

	// FOOTER_SIZE is the size of the footer in bytes.
	FOOTER_SIZE = 16
)

// Footer is the last 16 bytes of the SSTable file.
type Footer struct {
	FilterOffset uint64 // Offset where filter section starts (8 bytes)
	IndexOffset  uint64 // Offset where index section starts (8 bytes)
}

// Entry encoding in data blocks:
//
//   keyLen   (varint)
//   valueLen (varint)
//   seq      (uint64)
//   flags    (uint8)  // 0 = value present, 1 = tombstone
//   key      ([]byte)
//   value    ([]byte) // omitted if tombstone

// Index entry encoding:
//
//   keyLen       (varint)
//   blockOffset  (uint64)
//   key          ([]byte)  // first key in the block
