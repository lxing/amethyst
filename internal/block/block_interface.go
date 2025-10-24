package block

import "amethyst/internal/common"

// Block provides fast key lookups within a parsed data block.
type Block interface {
	// Get returns the entry for the given key, or nil if not found in this block.
	Get(key []byte) (*common.Entry, error)
}
