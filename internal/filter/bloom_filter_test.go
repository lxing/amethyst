package filter

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptimalBloomFilterParams(t *testing.T) {
	tests := []struct {
		n            uint64
		p            float64
		expectedK    uint32
		expectedMMin uint64 // m should be at least this
	}{
		{100, 0.01, 7, 900},   // ~958 bits for 100 elements at 1% FP
		{1000, 0.01, 7, 9000}, // ~9585 bits for 1000 elements at 1% FP
		{100, 0.001, 10, 1400}, // ~1438 bits for 100 elements at 0.1% FP
	}

	for _, tt := range tests {
		k, m := OptimalBloomFilterParams(tt.n, tt.p)
		require.Equal(t, tt.expectedK, k, "k for n=%d p=%f", tt.n, tt.p)
		require.GreaterOrEqual(t, m, tt.expectedMMin, "m for n=%d p=%f should be >= %d", tt.n, tt.p, tt.expectedMMin)
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	n := uint64(1000)
	p := 0.01 // 1% target false positive rate

	// Compute optimal parameters
	k, m := OptimalBloomFilterParams(n, p)
	bf := NewBloomFilter(k, m).(*bloomFilter)

	// Add n keys (using format "key-%d")
	for i := uint64(0); i < n; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		bf.Add(key)
	}

	// Test a large number of keys that were NOT added
	testCount := 10000
	falsePositives := 0
	for i := uint64(n); i < n+uint64(testCount); i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	// Calculate observed false positive rate
	observedFP := float64(falsePositives) / float64(testCount)

	// Verify false positive rate is within 3x of target
	maxAcceptableFP := p * 3.0
	require.LessOrEqual(t, observedFP, maxAcceptableFP,
		"False positive rate %.4f exceeds 3x target (%.4f). k=%d, m=%d, n=%d",
		observedFP, maxAcceptableFP, k, m, n)

	t.Logf("False positive rate: %.4f (target: %.4f, max: %.4f), k=%d, m=%d",
		observedFP, p, maxAcceptableFP, k, m)
}

func TestBloomFilterAddAndMayContain(t *testing.T) {
	bf := NewBloomFilter(3, 1000).(*bloomFilter)

	// Test keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("test"),
		[]byte("bloom"),
	}

	// Add keys
	for _, key := range keys {
		bf.Add(key)
	}

	// All added keys should be found
	for _, key := range keys {
		require.True(t, bf.MayContain(key), "added key %s should be found", key)
	}

	// Keys not added might return false (but could have false positives)
	notAddedKeys := [][]byte{
		[]byte("notadded1"),
		[]byte("notadded2"),
		[]byte("missing"),
	}

	// We can't assert false here due to false positives, but we can verify
	// that at least the method doesn't panic
	for _, key := range notAddedKeys {
		_ = bf.MayContain(key)
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(5, 10000).(*bloomFilter)

	// Add a large number of keys
	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		bf.Add(keys[i])
	}

	// Verify all added keys are found (no false negatives)
	for i, key := range keys {
		require.True(t, bf.MayContain(key), "key %d should be found", i)
	}
}

func TestBloomFilterWriteAndRead(t *testing.T) {
	// Create and populate a bloom filter
	original := NewBloomFilter(4, 1000).(*bloomFilter)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("test"),
	}
	for _, key := range keys {
		original.Add(key)
	}

	// Serialize
	var buf bytes.Buffer
	n, err := WriteBloomFilter(&buf, original)
	require.NoError(t, err, "WriteBloomFilter failed")

	// Check bytes written: 4 (k) + 8 (m) + bitmap bytes
	expectedSize := 4 + 8 + int((1000+7)/8)
	require.Equal(t, expectedSize, n, "WriteBloomFilter bytes written")

	// Deserialize
	restored, err := ReadBloomFilter(&buf)
	require.NoError(t, err, "ReadBloomFilter failed")

	// Verify all keys are still found
	for _, key := range keys {
		require.True(t, restored.MayContain(key), "key %s should be found in restored filter", key)
	}
}

func TestBloomFilterFromBytes(t *testing.T) {
	// Create and populate original
	original := NewBloomFilter(3, 500).(*bloomFilter)
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	for _, key := range keys {
		original.Add(key)
	}

	// Get bytes from bitmap
	data := original.bitmap.Bytes()

	// Reconstruct from bytes
	restored := NewBloomFilterFromBytes(3, 500, data).(*bloomFilter)

	// Verify parameters
	require.Equal(t, original.k, restored.k, "k should match")
	require.Equal(t, original.m, restored.m, "m should match")

	// Verify all keys are found
	for _, key := range keys {
		require.True(t, restored.MayContain(key), "key %s should be found", key)
	}
}

func TestBloomFilterHash(t *testing.T) {
	bf := NewBloomFilter(2, 100).(*bloomFilter)

	// Test that hash produces consistent results
	key := []byte("testkey")
	h1a, h2a := bf.hash(key)
	h1b, h2b := bf.hash(key)

	require.Equal(t, h1a, h1b, "hash1 should be consistent")
	require.Equal(t, h2a, h2b, "hash2 should be consistent")

	// Test that different keys produce different hashes
	key2 := []byte("testkey2")
	h1c, h2c := bf.hash(key2)

	require.NotEqual(t, h1a, h1c, "different keys should produce different hash1")
	require.NotEqual(t, h2a, h2c, "different keys should produce different hash2")

	// Test that hash2 is never zero
	require.NotEqual(t, uint64(0), h2a, "hash2 should not be zero")
	require.NotEqual(t, uint64(0), h2c, "hash2 should not be zero")
}
