package block_cache

import "amethyst/internal/common"

// lruCache is a placeholder LRU cache implementation.
type lruCache struct{}

func (c *lruCache) Get(fileNo common.FileNo, blockNo common.BlockNo) ([]byte, bool) {
	return nil, false
}

func (c *lruCache) Put(fileNo common.FileNo, blockNo common.BlockNo, data []byte) {
	// No-op
}
