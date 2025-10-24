package sstable

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
