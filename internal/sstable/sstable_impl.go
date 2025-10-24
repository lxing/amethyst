package sstable

// SSTable File Layout:
//
// ┌─────────────────┐
// │  Data Block 0   │  block.BLOCK_SIZE entries, sorted by key (no duplicates)
// ├─────────────────┤
// │  Data Block 1   │  block.BLOCK_SIZE entries
// ├─────────────────┤
// │      ...        │
// ├─────────────────┤
// │  Data Block N   │  block.BLOCK_SIZE entries (may have fewer in last block)
// ├─────────────────┤
// │ Filter Section  │  Bloom filter
// ├─────────────────┤
// │  Index Section  │  Array of {firstKey, blockOffset} entries
// ├─────────────────┤
// │     Footer      │  16 bytes: filterOffset + indexOffset
// └─────────────────┘

const (
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
