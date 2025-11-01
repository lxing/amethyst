package bitmap

import (
	"fmt"
)

type Bitmap struct {
	data    []byte
	numBits uint32
}

func NewBitmap(numBits uint32) *Bitmap {
	// Calculate number of bytes needed: ceil(numBits / 8)
	numBytes := (numBits + 7) / 8
	return &Bitmap{
		data:    make([]byte, numBytes),
		numBits: numBits,
	}
}

func NewBitmapFromBytes(numBits uint32, data []byte) *Bitmap {
	return &Bitmap{
		data:    data,
		numBits: numBits,
	}
}

func (b *Bitmap) Add(i uint32) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] |= (1 << bitIdx)
}

func (b *Bitmap) Remove(i uint32) {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	b.data[byteIdx] &= ^(1 << bitIdx)
}

func (b *Bitmap) Contains(i uint32) bool {
	if i >= b.numBits {
		panic(fmt.Sprintf("bitmap: index %d out of range [0, %d)", i, b.numBits))
	}
	byteIdx := i / 8
	bitIdx := i % 8
	return (b.data[byteIdx] & (1 << bitIdx)) != 0
}

func (b *Bitmap) Bytes() []byte {
	return b.data
}
