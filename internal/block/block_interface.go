package block

import "amethyst/internal/common"

// BLOCK_SIZE is the maximum number of entries per data block.
// The last block in an SSTable may contain fewer entries.
const BLOCK_SIZE = 64

// Block provides fast key lookups within a parsed data block.
type Block interface {
	// Get returns the entry for the given key. Returns (entry, true) if found, (nil, false) if not found.
	Get(key []byte) (*common.Entry, bool)

	// Len returns the number of entries in this block.
	Len() int
}
