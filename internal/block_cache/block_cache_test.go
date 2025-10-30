package block_cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"amethyst/internal/block"
	"amethyst/internal/common"
)

func TestBlockCacheBasic(t *testing.T) {
	cache := NewBlockCache(2)

	// Cache miss
	blk, found := cache.Get(1, 0)
	require.False(t, found)
	require.Nil(t, blk)

	// Put a block
	b1 := &block.Block{}
	cache.Put(1, 0, b1)

	// Cache hit
	blk, found = cache.Get(1, 0)
	require.True(t, found)
	require.Equal(t, b1, blk)
}

func TestBlockCacheLRUEviction(t *testing.T) {
	cache := NewBlockCache(2)

	b1 := &block.Block{}
	b2 := &block.Block{}
	b3 := &block.Block{}

	// Fill cache to capacity
	cache.Put(1, 0, b1)
	cache.Put(2, 0, b2)

	// Both should be in cache
	_, found := cache.Get(1, 0)
	require.True(t, found)
	_, found = cache.Get(2, 0)
	require.True(t, found)

	// Add third block, should evict first (LRU)
	cache.Put(3, 0, b3)

	// Block 1 should be evicted
	_, found = cache.Get(1, 0)
	require.False(t, found)

	// Blocks 2 and 3 should still be present
	_, found = cache.Get(2, 0)
	require.True(t, found)
	_, found = cache.Get(3, 0)
	require.True(t, found)
}

func TestBlockCacheLRUOrder(t *testing.T) {
	cache := NewBlockCache(2)

	b1 := &block.Block{}
	b2 := &block.Block{}
	b3 := &block.Block{}

	// Fill cache
	cache.Put(1, 0, b1)
	cache.Put(2, 0, b2)

	// Access block 1 (moves it to head)
	cache.Get(1, 0)

	// Add third block, should evict block 2 (now LRU)
	cache.Put(3, 0, b3)

	// Block 2 should be evicted
	_, found := cache.Get(2, 0)
	require.False(t, found)

	// Blocks 1 and 3 should still be present
	_, found = cache.Get(1, 0)
	require.True(t, found)
	_, found = cache.Get(3, 0)
	require.True(t, found)
}

func TestBlockCacheUpdate(t *testing.T) {
	cache := NewBlockCache(2)

	b1 := &block.Block{}
	b2 := &block.Block{}

	// Put a block
	cache.Put(1, 0, b1)

	// Update the same key
	cache.Put(1, 0, b2)

	// Should get updated block
	blk, found := cache.Get(1, 0)
	require.True(t, found)
	require.Equal(t, b2, blk)

	// Should still only have 1 entry
	require.Equal(t, 1, len(cache.blocks))
}

func TestBlockCacheConcurrent(t *testing.T) {
	cache := NewBlockCache(100)

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent puts and gets
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				fileNo := common.FileNo(id)
				blockNo := common.BlockNo(j)
				b := &block.Block{}

				cache.Put(fileNo, blockNo, b)
				cache.Get(fileNo, blockNo)
			}
		}(i)
	}

	wg.Wait()
}

func TestBlockCacheDifferentFiles(t *testing.T) {
	cache := NewBlockCache(10)

	b1 := &block.Block{}
	b2 := &block.Block{}

	// Same blockNo, different fileNo
	cache.Put(1, 0, b1)
	cache.Put(2, 0, b2)

	// Should be able to get both
	blk1, found := cache.Get(1, 0)
	require.True(t, found)
	require.Equal(t, b1, blk1)

	blk2, found := cache.Get(2, 0)
	require.True(t, found)
	require.Equal(t, b2, blk2)

	// Should have 2 entries
	require.Equal(t, 2, len(cache.blocks))
}
