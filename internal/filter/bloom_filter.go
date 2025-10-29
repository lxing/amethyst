package filter

import (
	"hash/fnv"
	"io"
	"math"

	"amethyst/internal/bitmap"
	"amethyst/internal/common"
)

// bloomFilter implements a space-efficient probabilistic data structure
// for set membership testing with no false negatives.
type bloomFilter struct {
	bitmap bitmap.Bitmap
	k      uint32 // number of hash functions
	m      uint32 // number of bits in bitmap
}

var _ Filter = (*bloomFilter)(nil)

// OptimalBloomFilterParams computes optimal bloom filter parameters.
// n: expected number of elements to insert
// p: desired false positive rate (e.g., 0.01 for 1%)
// Returns: k (number of hash functions), m (number of bits)
func OptimalBloomFilterParams(n uint32, p float64) (k uint32, m uint32) {
	// m = -n * ln(p) / (ln(2)^2)
	m = uint32(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))

	// k = (m/n) * ln(2)
	k = uint32(math.Ceil(float64(m) / float64(n) * math.Ln2))

	// Ensure at least 1 hash function
	if k < 1 {
		k = 1
	}

	return k, m
}

// NewBloomFilter creates a new bloom filter.
// k: number of hash functions
// m: number of bits in the bitmap
func NewBloomFilter(k uint32, m uint32) Filter {
	return &bloomFilter{
		bitmap: bitmap.NewBitmap(m),
		k:      k,
		m:      m,
	}
}

// NewBloomFilterFromBytes reconstructs a bloom filter from serialized data.
func NewBloomFilterFromBytes(k uint32, m uint32, data []byte) Filter {
	return &bloomFilter{
		bitmap: bitmap.NewBitmapFromBytes(m, data),
		k:      k,
		m:      m,
	}
}

// Add inserts a key into the bloom filter.
func (bf *bloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := uint32((h1 + uint64(i)*h2) % uint64(bf.m))
		bf.bitmap.Add(pos)
	}
}

// MayContain returns true if the key might be in the set.
// Returns false if the key is definitely NOT in the set.
func (bf *bloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := uint32((h1 + uint64(i)*h2) % uint64(bf.m))
		if !bf.bitmap.Contains(pos) {
			return false
		}
	}
	return true
}

// hash computes two hash values using FNV-1a for double hashing.
func (bf *bloomFilter) hash(key []byte) (uint64, uint64) {
	// First hash
	h1 := fnv.New64a()
	h1.Write(key)
	hash1 := h1.Sum64()

	// Second hash (with a different seed/approach)
	h2 := fnv.New64a()
	h2.Write(key)
	h2.Write([]byte{0x01}) // Add a byte to differentiate
	hash2 := h2.Sum64()

	// Ensure hash2 is non-zero to avoid infinite loops
	if hash2 == 0 {
		hash2 = 1
	}

	return hash1, hash2
}

// WriteBloomFilter serializes a bloom filter to a writer.
// Format: [k: uint32][m: uint32][bitmap data: []byte]
func WriteBloomFilter(w io.Writer, f Filter) (int, error) {
	bf := f.(*bloomFilter)
	total := 0

	// Write k (number of hash functions)
	n, err := common.WriteUint32(w, bf.k)
	total += n
	if err != nil {
		return total, err
	}

	// Write m (number of bits)
	n, err = common.WriteUint32(w, bf.m)
	total += n
	if err != nil {
		return total, err
	}

	// Write bitmap data
	n, err = common.WriteBytes(w, bf.bitmap.Bytes())
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadBloomFilter deserializes a bloom filter from a reader.
func ReadBloomFilter(r io.Reader) (Filter, error) {
	// Read k
	k, err := common.ReadUint32(r)
	if err != nil {
		return nil, err
	}

	// Read m
	m, err := common.ReadUint32(r)
	if err != nil {
		return nil, err
	}

	// Calculate bitmap size and read data
	numBytes := (m + 7) / 8
	data, err := common.ReadBytes(r, uint64(numBytes))
	if err != nil {
		return nil, err
	}

	return NewBloomFilterFromBytes(k, m, data), nil
}
