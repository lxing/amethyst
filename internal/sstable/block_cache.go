package sstable

import (
	"sync"

	"amethyst/internal/block"
	"amethyst/internal/common"
	"amethyst/internal/dsa/lru"
)

type BlockKey struct {
	FileNo  common.FileNo
	BlockNo common.BlockNo
}

// BlockCache wraps LRUCache with a mutex for thread-safe block caching.
type BlockCache struct {
	mu    sync.Mutex
	cache *lru.LRUCache[BlockKey, *block.Block]
}

func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		cache: lru.New[BlockKey, *block.Block](capacity),
	}
}

func (bc *BlockCache) Get(key BlockKey) (value *block.Block, ok bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.cache.Get(key)
}

func (bc *BlockCache) Put(key BlockKey, value *block.Block) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.cache.Put(key, value)
}
