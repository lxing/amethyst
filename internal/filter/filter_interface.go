package filter

// Filter provides fast negative lookups for keys in an SSTable.
// A bloom filter can definitively say a key is NOT present, but can only
// say a key MIGHT be present (false positives possible, false negatives not).
type Filter interface {
	// MayContain returns true if the key might be in the SSTable.
	// Returns false if the key is definitely NOT in the SSTable.
	MayContain(key []byte) bool
}
