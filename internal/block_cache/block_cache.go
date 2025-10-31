package block_cache

import (
	"sync"

	"amethyst/internal/block"
	"amethyst/internal/common"
)

// blockKey uniquely identifies a block across all SSTables.
type blockKey struct {
	fileNo  common.FileNo
	blockNo common.BlockNo
}

type lruNode struct {
	key   blockKey
	block *block.Block
	prev  *lruNode
	next  *lruNode
}

type BlockCache struct {
	mu       sync.Mutex
	capacity int
	blocks   map[blockKey]*lruNode
	head     *lruNode // Most recently used (sentinel)
	tail     *lruNode // Least recently used (sentinel)
}

func NewBlockCache(capacity int) *BlockCache {
	c := &BlockCache{
		capacity: capacity,
		blocks:   make(map[blockKey]*lruNode),
		head:     &lruNode{},
		tail:     &lruNode{},
	}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

// TODO: generics
func (c *BlockCache) Get(fileNo common.FileNo, blockNo common.BlockNo) (*block.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := blockKey{fileNo, blockNo}
	node, found := c.blocks[key]
	if !found {
		return nil, false
	}

	// Remove from current position
	node.prev.next = node.next
	node.next.prev = node.prev

	// Insert at head
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	return node.block, true
}

func (c *BlockCache) Put(fileNo common.FileNo, blockNo common.BlockNo, b *block.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := blockKey{fileNo, blockNo}

	// Check if already exists (update case)
	if node, found := c.blocks[key]; found {
		node.block = b
		// Move to head - inline list manipulation
		node.prev.next = node.next
		node.next.prev = node.prev
		node.next = c.head.next
		node.prev = c.head
		c.head.next.prev = node
		c.head.next = node
		return
	}

	// New entry - create node and insert at head
	node := &lruNode{
		key:   key,
		block: b,
	}
	c.blocks[key] = node
	// Insert at head - inline
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	// Evict if over capacity
	if len(c.blocks) > c.capacity {
		// Remove tail (LRU) - inline
		victim := c.tail.prev
		victim.prev.next = c.tail
		c.tail.prev = victim.prev
		delete(c.blocks, victim.key)
	}
}
