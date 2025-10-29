package bitmap

import (
	"bytes"
	"testing"
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
		if uint64(len(b.data)) != tt.expectedSize {
			t.Errorf("NewBitmap(%d): expected %d bytes, got %d", tt.numBits, tt.expectedSize, len(b.data))
		}
		if b.numBits != tt.numBits {
			t.Errorf("NewBitmap(%d): expected numBits=%d, got %d", tt.numBits, tt.numBits, b.numBits)
		}
		// Verify all bits are 0
		for i := uint64(0); i < tt.numBits; i++ {
			if b.Contains(i) {
				t.Errorf("NewBitmap(%d): bit %d should be 0", tt.numBits, i)
			}
		}
	}
}

func TestAddAndContains(t *testing.T) {
	b := NewBitmap(64)

	// Initially all bits should be 0
	for i := uint64(0); i < 64; i++ {
		if b.Contains(i) {
			t.Errorf("bit %d should initially be 0", i)
		}
	}

	// Add some bits
	positions := []uint64{0, 1, 7, 8, 15, 16, 31, 32, 63}
	for _, pos := range positions {
		b.Add(pos)
	}

	// Check added bits are set
	for _, pos := range positions {
		if !b.Contains(pos) {
			t.Errorf("bit %d should be set after Add", pos)
		}
	}

	// Check other bits are still 0
	for i := uint64(0); i < 64; i++ {
		shouldBeSet := false
		for _, pos := range positions {
			if i == pos {
				shouldBeSet = true
				break
			}
		}
		if shouldBeSet {
			if !b.Contains(i) {
				t.Errorf("bit %d should be set", i)
			}
		} else {
			if b.Contains(i) {
				t.Errorf("bit %d should not be set", i)
			}
		}
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
		if !b.Contains(i) {
			t.Errorf("bit %d should be set", i)
		}
	}

	// Remove some bits
	positions := []uint64{0, 7, 8, 15, 31, 63}
	for _, pos := range positions {
		b.Remove(pos)
	}

	// Check removed bits are cleared
	for _, pos := range positions {
		if b.Contains(pos) {
			t.Errorf("bit %d should be cleared after Remove", pos)
		}
	}

	// Check other bits are still set
	for i := uint64(0); i < 64; i++ {
		shouldBeCleared := false
		for _, pos := range positions {
			if i == pos {
				shouldBeCleared = true
				break
			}
		}
		if shouldBeCleared {
			if b.Contains(i) {
				t.Errorf("bit %d should be cleared", i)
			}
		} else {
			if !b.Contains(i) {
				t.Errorf("bit %d should still be set", i)
			}
		}
	}
}

func TestAddIdempotent(t *testing.T) {
	b := NewBitmap(64)

	// Add same bit multiple times
	b.Add(42)
	b.Add(42)
	b.Add(42)

	if !b.Contains(42) {
		t.Error("bit 42 should be set")
	}

	// Verify only that bit is set
	for i := uint64(0); i < 64; i++ {
		if i == 42 {
			if !b.Contains(i) {
				t.Errorf("bit %d should be set", i)
			}
		} else {
			if b.Contains(i) {
				t.Errorf("bit %d should not be set", i)
			}
		}
	}
}

func TestRemoveIdempotent(t *testing.T) {
	b := NewBitmap(64)

	// Add a bit
	b.Add(42)
	if !b.Contains(42) {
		t.Error("bit 42 should be set")
	}

	// Remove it multiple times
	b.Remove(42)
	b.Remove(42)
	b.Remove(42)

	if b.Contains(42) {
		t.Error("bit 42 should be cleared")
	}
}

func TestBoundsChecking(t *testing.T) {
	b := NewBitmap(64)

	// Test Add out of bounds
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Add(64) should panic")
			}
		}()
		b.Add(64)
	}()

	// Test Contains out of bounds
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Contains(64) should panic")
			}
		}()
		b.Contains(64)
	}()

	// Test Remove out of bounds
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Remove(64) should panic")
			}
		}()
		b.Remove(64)
	}()
}

func TestWriteAndReadBitmap(t *testing.T) {
	// Create and populate a bitmap
	original := NewBitmap(100)
	positions := []uint64{0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 99}
	for _, pos := range positions {
		original.Add(pos)
	}

	// Write to buffer
	var buf bytes.Buffer
	n, err := WriteBitmap(&buf, original.(*bitmapImpl))
	if err != nil {
		t.Fatalf("WriteBitmap failed: %v", err)
	}

	// Check bytes written
	expectedSize := 8 + (100+7)/8 // 8 bytes numBits + 13 bytes data
	if n != expectedSize {
		t.Errorf("WriteBitmap: expected %d bytes written, got %d", expectedSize, n)
	}

	// Read back
	restored, err := ReadBitmap(&buf)
	if err != nil {
		t.Fatalf("ReadBitmap failed: %v", err)
	}

	// Verify all bits match
	for i := uint64(0); i < 100; i++ {
		if original.Contains(i) != restored.Contains(i) {
			t.Errorf("bit %d mismatch: original=%v restored=%v", i, original.Contains(i), restored.Contains(i))
		}
	}
}

func TestWriteAndReadEmptyBitmap(t *testing.T) {
	original := NewBitmap(0)

	var buf bytes.Buffer
	_, err := WriteBitmap(&buf, original.(*bitmapImpl))
	if err != nil {
		t.Fatalf("WriteBitmap failed: %v", err)
	}

	restored, err := ReadBitmap(&buf)
	if err != nil {
		t.Fatalf("ReadBitmap failed: %v", err)
	}

	restoredImpl := restored.(*bitmapImpl)
	if restoredImpl.numBits != 0 {
		t.Errorf("expected numBits=0, got %d", restoredImpl.numBits)
	}
	if len(restoredImpl.data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(restoredImpl.data))
	}
}
