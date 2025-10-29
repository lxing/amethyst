package filter

// Filter provides fast negative lookups for set membership testing.
// A filter can definitively say a key is NOT present, but can only
// say a key MIGHT be present (false positives possible, false negatives not).
type Filter interface {
	// Add inserts a key into the filter.
	Add(key []byte)

	// MayContain returns true if the key might be in the set.
	// Returns false if the key is definitely NOT in the set.
	MayContain(key []byte) bool
}
