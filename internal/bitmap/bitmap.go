package bitmap

import (
	"fmt"
)

// bitmapImpl is a concrete implementation of the Bitmap interface.
type bitmapImpl struct {
	data    []byte // Backing storage: each byte stores 8 bits
	numBits uint32 // Total number of bits in the bitmap
}

// NewBitmap creates a new bitmap with the specified number of bits.
// All bits are initialized to 0.
func NewBitmap(numBits uint32) Bitmap {
	// Calculate number of bytes needed: ceil(numBits / 8)
	numBytes := (numBits + 7) / 8
	return &bitmapImpl{
		data:    make([]byte, numBytes),
		numBits: numBits,
	}
}

// NewBitmapFromBytes creates a bitmap from existing byte data.
func NewBitmapFromBytes(numBits uint32, data []byte) Bitmap {
	return &bitmapImpl{
		data:    data,
		numBits: numBits,
	}
}

// Add sets the bit at position i to 1 (adds i to the set).
func (b *bitmapImpl) Add(i uint32) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] |= (1 << bitIdx)
}

// Remove sets the bit at position i to 0 (removes i from the set).
func (b *bitmapImpl) Remove(i uint32) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] &= ^(1 << bitIdx)
}

// Contains returns true if bit at position i is set (i is in the set).
func (b *bitmapImpl) Contains(i uint32) bool {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	return (b.data[byteIdx] & (1 << bitIdx)) != 0
}

// Bytes returns the underlying byte array.
func (b *bitmapImpl) Bytes() []byte {
	return b.data
}
