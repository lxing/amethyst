package heap

type item[K, V any] struct {
	key   K
	value V
}

type Heap[K, V any] struct {
	items []item[K, V]
	cmp   func(K, K) int
}

func New[K, V any](cmp func(K, K) int) *Heap[K, V] {
	return &Heap[K, V]{
		items: make([]item[K, V], 0),
		cmp:   cmp,
	}
}

func (h *Heap[K, V]) Len() int {
	return len(h.items)
}

func (h *Heap[K, V]) Peek() (K, bool) {
	var zero K
	if len(h.items) == 0 {
		return zero, false
	}
	return h.items[0].key, true
}

func (h *Heap[K, V]) Push(key K, value V) {
	h.items = append(h.items, item[K, V]{key: key, value: value})
	h.heapifyUp(len(h.items) - 1)
}

func (h *Heap[K, V]) Pop() (K, V, bool) {
	var zeroK K
	var zeroV V

	if len(h.items) == 0 {
		return zeroK, zeroV, false
	}

	top := h.items[0]

	lastIdx := len(h.items) - 1
	h.items[0] = h.items[lastIdx]
	h.items = h.items[:lastIdx]

	if len(h.items) > 0 {
		h.heapifyDown(0)
	}

	return top.key, top.value, true
}

func (h *Heap[K, V]) heapifyUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if h.cmp(h.items[idx].key, h.items[parent].key) >= 0 {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *Heap[K, V]) heapifyDown(idx int) {
	for {
		smallest := idx
		left := 2*idx + 1
		right := 2*idx + 2

		if left < len(h.items) &&
			h.cmp(h.items[left].key, h.items[smallest].key) < 0 {
			smallest = left
		}
		if right < len(h.items) &&
			h.cmp(h.items[right].key, h.items[smallest].key) < 0 {
			smallest = right
		}

		if smallest == idx {
			break
		}

		h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
		idx = smallest
	}
}
