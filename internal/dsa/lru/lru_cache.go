package lru

import "sync"

type lruNode[K comparable, V any] struct {
	key   K
	value V
	prev  *lruNode[K, V]
	next  *lruNode[K, V]
}

type LRUCache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	items    map[K]*lruNode[K, V]
	head     *lruNode[K, V]
	tail     *lruNode[K, V]
}

func New[K comparable, V any](capacity int) *LRUCache[K, V] {
	c := &LRUCache[K, V]{
		capacity: capacity,
		items:    make(map[K]*lruNode[K, V]),
		head:     &lruNode[K, V]{},
		tail:     &lruNode[K, V]{},
	}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	node, found := c.items[key]
	if !found {
		return zero, false
	}

	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	return node.value, true
}

func (c *LRUCache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, found := c.items[key]; found {
		node.value = value
		node.prev.next = node.next
		node.next.prev = node.prev
		node.next = c.head.next
		node.prev = c.head
		c.head.next.prev = node
		c.head.next = node
		return
	}

	node := &lruNode[K, V]{
		key:   key,
		value: value,
	}
	c.items[key] = node
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	if len(c.items) > c.capacity {
		victim := c.tail.prev
		victim.prev.next = c.tail
		c.tail.prev = victim.prev
		delete(c.items, victim.key)
	}
}
