package block_cache

import "amethyst/internal/common"

// BlockCache provides shared LRU block caching across multiple SSTables.
type BlockCache interface {
	// Get retrieves a block from the cache. Returns (data, true) if found, (nil, false) if not.
	Get(fileNo common.FileNo, blockNo common.BlockNo) ([]byte, bool)

	// Put stores a block in the cache.
	Put(fileNo common.FileNo, blockNo common.BlockNo, data []byte)
}
