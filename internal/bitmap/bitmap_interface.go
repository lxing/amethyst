package bitmap

// Bitmap is a set interface backed by a space-efficient bit array.
// We will use it to support a bloom filter.
type Bitmap interface {
	// Add sets the bit at position i to 1 (adds i to the set).
	Add(i uint64)

	// Remove sets the bit at position i to 0 (removes i from the set).
	Remove(i uint64)

	// Contains returns true if bit at position i is set (i is in the set).
	Contains(i uint64) bool
}
