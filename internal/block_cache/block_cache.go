package block_cache

import "amethyst/internal/common"

// BlockCache provides shared LRU block caching across multiple SSTables.
type BlockCache interface {
	// Get retrieves a block from the cache, or nil if not present.
	Get(fileNo common.FileNo, blockNo common.BlockNo) []byte

	// Put stores a block in the cache.
	Put(fileNo common.FileNo, blockNo common.BlockNo, data []byte)
}
