package block_cache

import (
	"amethyst/internal/block"
	"amethyst/internal/common"
)

// BlockCache is a placeholder LRU cache implementation.
type BlockCache struct{}

// NewBlockCache creates a new block cache.
func NewBlockCache() *BlockCache {
	return &BlockCache{}
}

func (c *BlockCache) Get(fileNo common.FileNo, blockNo common.BlockNo) (*block.Block, bool) {
	return nil, false
}

func (c *BlockCache) Put(fileNo common.FileNo, blockNo common.BlockNo, b *block.Block) {
	// No-op
}
