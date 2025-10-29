package bitmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBitmap(t *testing.T) {
	tests := []struct {
		numBits      uint64
		expectedSize uint64
	}{
		{0, 0},
		{1, 1},
		{8, 1},
		{9, 2},
		{16, 2},
		{17, 3},
		{64, 8},
		{65, 9},
	}

	for _, tt := range tests {
		b := NewBitmap(tt.numBits).(*bitmapImpl)
		require.Equal(t, tt.expectedSize, uint64(len(b.data)), "NewBitmap(%d) data size", tt.numBits)
		require.Equal(t, tt.numBits, b.numBits, "NewBitmap(%d) numBits", tt.numBits)

		// Verify all bits are 0
		for i := uint64(0); i < tt.numBits; i++ {
			require.False(t, b.Contains(i), "NewBitmap(%d): bit %d should be 0", tt.numBits, i)
		}
	}
}

func TestAddAndContains(t *testing.T) {
	b := NewBitmap(64)

	// Initially all bits should be 0
	for i := uint64(0); i < 64; i++ {
		require.False(t, b.Contains(i), "bit %d should initially be 0", i)
	}

	// Add some bits
	positions := map[uint64]struct{}{
		0: {}, 1: {}, 7: {}, 8: {}, 15: {}, 16: {}, 31: {}, 32: {}, 63: {},
	}
	for pos := range positions {
		b.Add(pos)
	}

	// Verify all bits have correct status
	for i := uint64(0); i < 64; i++ {
		_, shouldBeSet := positions[i]
		require.Equal(t, shouldBeSet, b.Contains(i), "bit %d set status", i)
	}
}

func TestRemove(t *testing.T) {
	b := NewBitmap(64)

	// Add all bits
	for i := uint64(0); i < 64; i++ {
		b.Add(i)
	}

	// Verify all bits are set
	for i := uint64(0); i < 64; i++ {
		require.True(t, b.Contains(i), "bit %d should be set", i)
	}

	// Remove some bits
	positions := map[uint64]struct{}{
		0: {}, 7: {}, 8: {}, 15: {}, 31: {}, 63: {},
	}
	for pos := range positions {
		b.Remove(pos)
	}

	// Verify all bits have correct status
	for i := uint64(0); i < 64; i++ {
		_, shouldBeCleared := positions[i]
		require.Equal(t, !shouldBeCleared, b.Contains(i), "bit %d set status", i)
	}
}

func TestIdempotent(t *testing.T) {
	b := NewBitmap(64)

	// Add same bit multiple times
	b.Add(42)
	b.Add(42)
	b.Add(42)

	require.True(t, b.Contains(42), "bit 42 should be set")

	// Verify only that bit is set
	for i := uint64(0); i < 64; i++ {
		if i == 42 {
			require.True(t, b.Contains(i), "bit %d should be set", i)
		} else {
			require.False(t, b.Contains(i), "bit %d should not be set", i)
		}
	}

	// Remove it multiple times
	b.Remove(42)
	b.Remove(42)
	b.Remove(42)

	require.False(t, b.Contains(42), "bit 42 should be cleared")
}

func TestBoundsChecking(t *testing.T) {
	b := NewBitmap(64)

	// Test Add out of bounds
	require.Panics(t, func() {
		b.Add(64)
	}, "Add(64) should panic")

	// Test Contains out of bounds
	require.Panics(t, func() {
		b.Contains(64)
	}, "Contains(64) should panic")

	// Test Remove out of bounds
	require.Panics(t, func() {
		b.Remove(64)
	}, "Remove(64) should panic")
}

func TestBytesAndFromBytes(t *testing.T) {
	// Create and populate a bitmap
	original := NewBitmap(100)
	positions := map[uint64]struct{}{
		0: {}, 1: {}, 7: {}, 8: {}, 15: {}, 16: {}, 31: {}, 32: {}, 63: {}, 64: {}, 99: {},
	}
	for pos := range positions {
		original.Add(pos)
	}

	// Get bytes
	data := original.Bytes()
	expectedSize := (100 + 7) / 8 // 13 bytes
	require.Equal(t, int(expectedSize), len(data), "Bytes() length")

	// Reconstruct from bytes
	restored := NewBitmapFromBytes(100, data)

	// Verify all bits match
	for i := uint64(0); i < 100; i++ {
		require.Equal(t, original.Contains(i), restored.Contains(i), "bit %d mismatch", i)
	}
}

