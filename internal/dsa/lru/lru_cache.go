package lru

type lruNode[K comparable, V any] struct {
	key   K
	value V
	prev  *lruNode[K, V]
	next  *lruNode[K, V]
}

// LRUCache is a non-thread-safe LRU cache.
// Callers must provide their own synchronization if needed.
type LRUCache[K comparable, V any] struct {
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

func (c *LRUCache[K, V]) Get(key K) (value V, ok bool) {
	node, found := c.items[key]
	if !found {
		return
	}

	// Move to front (most recently used)
	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	return node.value, true
}

func (c *LRUCache[K, V]) Put(key K, value V) {
	if node, found := c.items[key]; found {
		// Update existing entry
		node.value = value
		node.prev.next = node.next
		node.next.prev = node.prev
		node.next = c.head.next
		node.prev = c.head
		c.head.next.prev = node
		c.head.next = node
		return
	}

	// Insert new entry
	node := &lruNode[K, V]{
		key:   key,
		value: value,
	}
	c.items[key] = node
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node

	// Evict LRU if over capacity
	if len(c.items) > c.capacity {
		victim := c.tail.prev
		victim.prev.next = c.tail
		c.tail.prev = victim.prev
		delete(c.items, victim.key)
	}
}
