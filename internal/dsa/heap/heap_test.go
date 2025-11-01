package heap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func intCmp(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func TestHeapEmpty(t *testing.T) {
	h := New[int, string](intCmp)
	require.Equal(t, 0, h.Len())

	// Peek on empty heap
	key, ok := h.Peek()
	require.False(t, ok)
	require.Equal(t, 0, key) // zero value

	// Pop on empty heap
	key, val, ok := h.Pop()
	require.False(t, ok)
	require.Equal(t, 0, key)    // zero value
	require.Equal(t, "", val)   // zero value
}

func TestHeapSingleElement(t *testing.T) {
	h := New[int, string](intCmp)

	h.Push(42, "answer")
	require.Equal(t, 1, h.Len())

	// Peek doesn't remove
	key, ok := h.Peek()
	require.True(t, ok)
	require.Equal(t, 42, key)
	require.Equal(t, 1, h.Len())

	// Pop removes
	key, val, ok := h.Pop()
	require.True(t, ok)
	require.Equal(t, 42, key)
	require.Equal(t, "answer", val)
	require.Equal(t, 0, h.Len())
}

func TestHeapMinProperty(t *testing.T) {
	h := New[int, string](intCmp)

	// Push in random order
	h.Push(5, "five")
	h.Push(3, "three")
	h.Push(7, "seven")
	h.Push(1, "one")
	h.Push(9, "nine")
	h.Push(2, "two")

	require.Equal(t, 6, h.Len())

	// Should always get minimum
	key, ok := h.Peek()
	require.True(t, ok)
	require.Equal(t, 1, key)

	// Pop in sorted order
	expected := []struct {
		key int
		val string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
		{5, "five"},
		{7, "seven"},
		{9, "nine"},
	}

	for _, exp := range expected {
		key, val, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, exp.key, key)
		require.Equal(t, exp.val, val)
	}

	require.Equal(t, 0, h.Len())
}

func TestHeapDuplicateKeys(t *testing.T) {
	h := New[int, string](intCmp)

	h.Push(5, "first")
	h.Push(5, "second")
	h.Push(3, "three")
	h.Push(5, "third")

	require.Equal(t, 4, h.Len())

	// First pop should be 3
	key, val, ok := h.Pop()
	require.True(t, ok)
	require.Equal(t, 3, key)
	require.Equal(t, "three", val)

	// Next three should all be 5 (order among duplicates undefined)
	for i := 0; i < 3; i++ {
		key, val, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, 5, key)
		// val could be any of "first", "second", "third"
		require.NotEmpty(t, val)
	}
}

func TestHeapInterleavedOps(t *testing.T) {
	h := New[int, int](intCmp)

	h.Push(10, 10)
	h.Push(5, 5)

	key, val, ok := h.Pop()
	require.True(t, ok)
	require.Equal(t, 5, key)
	require.Equal(t, 5, val)

	h.Push(3, 3)
	h.Push(8, 8)

	key, ok = h.Peek()
	require.True(t, ok)
	require.Equal(t, 3, key)

	h.Push(1, 1)

	key, ok = h.Peek()
	require.True(t, ok)
	require.Equal(t, 1, key)

	// Pop remaining in order
	expected := []int{1, 3, 8, 10}
	for _, exp := range expected {
		key, val, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, exp, key)
		require.Equal(t, exp, val)
	}
}

func TestHeapStringKeys(t *testing.T) {
	cmp := func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}

	h := New[string, int](cmp)

	h.Push("zebra", 26)
	h.Push("apple", 1)
	h.Push("mango", 13)
	h.Push("banana", 2)

	// Should pop in lexicographic order
	expected := []string{"apple", "banana", "mango", "zebra"}
	for _, exp := range expected {
		key, _, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, exp, key)
	}
}

func TestHeapReverseOrder(t *testing.T) {
	// Max heap: reverse comparison
	maxCmp := func(a, b int) int {
		if a > b {
			return -1
		}
		if a < b {
			return 1
		}
		return 0
	}

	h := New[int, string](maxCmp)

	h.Push(5, "five")
	h.Push(3, "three")
	h.Push(7, "seven")
	h.Push(1, "one")

	// Should pop in descending order
	expected := []int{7, 5, 3, 1}
	for _, exp := range expected {
		key, _, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, exp, key)
	}
}

func TestHeapLargeDataset(t *testing.T) {
	h := New[int, int](intCmp)

	// Push 1000 elements in reverse order
	for i := 1000; i > 0; i-- {
		h.Push(i, i)
	}

	require.Equal(t, 1000, h.Len())

	// Pop should give 1..1000 in order
	for i := 1; i <= 1000; i++ {
		key, val, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, i, key)
		require.Equal(t, i, val)
	}

	require.Equal(t, 0, h.Len())
}
