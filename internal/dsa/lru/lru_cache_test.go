package lru

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUCacheBasic(t *testing.T) {
	cache := New[int, string](2)

	// Cache miss
	val, found := cache.Get(1)
	require.False(t, found)
	require.Equal(t, "", val)

	// Put a value
	cache.Put(1, "hello")

	// Cache hit
	val, found = cache.Get(1)
	require.True(t, found)
	require.Equal(t, "hello", val)
}

func TestLRUCacheLRUEviction(t *testing.T) {
	cache := New[int, string](2)

	// Fill cache to capacity
	cache.Put(1, "first")
	cache.Put(2, "second")

	// Both should be in cache
	_, found := cache.Get(1)
	require.True(t, found)
	_, found = cache.Get(2)
	require.True(t, found)

	// Add third item, should evict first (LRU)
	cache.Put(3, "third")

	// Item 1 should be evicted
	_, found = cache.Get(1)
	require.False(t, found)

	// Items 2 and 3 should still be present
	_, found = cache.Get(2)
	require.True(t, found)
	_, found = cache.Get(3)
	require.True(t, found)
}

func TestLRUCacheLRUOrder(t *testing.T) {
	cache := New[int, string](2)

	// Fill cache
	cache.Put(1, "first")
	cache.Put(2, "second")

	// Access item 1 (moves it to head)
	cache.Get(1)

	// Add third item, should evict item 2 (now LRU)
	cache.Put(3, "third")

	// Item 2 should be evicted
	_, found := cache.Get(2)
	require.False(t, found)

	// Items 1 and 3 should still be present
	_, found = cache.Get(1)
	require.True(t, found)
	_, found = cache.Get(3)
	require.True(t, found)
}

func TestLRUCacheUpdate(t *testing.T) {
	cache := New[int, string](2)

	// Put a value
	cache.Put(1, "first")

	// Update the same key
	cache.Put(1, "updated")

	// Should get updated value
	val, found := cache.Get(1)
	require.True(t, found)
	require.Equal(t, "updated", val)

	// Should still only have 1 entry
	require.Equal(t, 1, len(cache.items))
}

func TestLRUCacheConcurrent(t *testing.T) {
	cache := New[int, string](100)

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent puts and gets
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := id*1000 + j
				cache.Put(key, "value")
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()
}
