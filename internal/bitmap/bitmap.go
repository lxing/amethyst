package bitmap

import (
	"fmt"
	"io"

	"amethyst/internal/common"
)

// bitmapImpl is a concrete implementation of the Bitmap interface.
type bitmapImpl struct {
	data    []byte // Backing storage: each byte stores 8 bits
	numBits uint64 // Total number of bits in the bitmap
}

// NewBitmap creates a new bitmap with the specified number of bits.
// All bits are initialized to 0.
func NewBitmap(numBits uint64) Bitmap {
	// Calculate number of bytes needed: ceil(numBits / 8)
	numBytes := (numBits + 7) / 8
	return &bitmapImpl{
		data:    make([]byte, numBytes),
		numBits: numBits,
	}
}

// Add sets the bit at position i to 1 (adds i to the set).
func (b *bitmapImpl) Add(i uint64) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] |= (1 << bitIdx)
}

// Remove sets the bit at position i to 0 (removes i from the set).
func (b *bitmapImpl) Remove(i uint64) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] &= ^(1 << bitIdx)
}

// Contains returns true if bit at position i is set (i is in the set).
func (b *bitmapImpl) Contains(i uint64) bool {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	return (b.data[byteIdx] & (1 << bitIdx)) != 0
}

// WriteBitmap serializes a bitmap to a writer.
// Format: [8 bytes: numBits][data bytes]
// Returns the number of bytes written.
func WriteBitmap(w io.Writer, b *bitmapImpl) (int, error) {
	total := 0

	// Write numBits (8 bytes)
	n, err := common.WriteUint64(w, b.numBits)
	total += n
	if err != nil {
		return total, err
	}

	// Write data bytes
	n, err = common.WriteBytes(w, b.data)
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadBitmap deserializes a bitmap from a reader.
// Format: [8 bytes: numBits][data bytes]
func ReadBitmap(r io.Reader) (Bitmap, error) {
	// Read numBits (8 bytes)
	numBits, err := common.ReadUint64(r)
	if err != nil {
		return nil, err
	}

	// Calculate expected data size
	numBytes := (numBits + 7) / 8

	// Read data bytes
	data, err := common.ReadBytes(r, numBytes)
	if err != nil {
		return nil, err
	}

	return &bitmapImpl{
		data:    data,
		numBits: numBits,
	}, nil
}
