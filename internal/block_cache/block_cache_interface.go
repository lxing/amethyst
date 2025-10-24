package block_cache

import (
	"amethyst/internal/block"
	"amethyst/internal/common"
)

// BlockCache provides shared LRU block caching across multiple SSTables.
type BlockCache interface {
	// Get retrieves a block from the cache. Returns (block, true) if found, (nil, false) if not.
	Get(fileNo common.FileNo, blockNo common.BlockNo) (block.Block, bool)

	// Put stores a block in the cache.
	Put(fileNo common.FileNo, blockNo common.BlockNo, b block.Block)
}
