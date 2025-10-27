package block_cache

import (
	"amethyst/internal/block"
	"amethyst/internal/common"
)

// lruCache is a placeholder LRU cache implementation.
type lruCache struct{}

var _ BlockCache = (*lruCache)(nil)

func (c *lruCache) Get(fileNo common.FileNo, blockNo common.BlockNo) (block.Block, bool) {
	return nil, false
}

func (c *lruCache) Put(fileNo common.FileNo, blockNo common.BlockNo, b block.Block) {
	// No-op
}
