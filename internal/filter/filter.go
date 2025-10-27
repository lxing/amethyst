package filter

// NoOpFilter is a filter that always returns true (no filtering).
// Used as a placeholder until bloom filter is implemented.
type NoOpFilter struct{}

var _ Filter = (*NoOpFilter)(nil)

// MayContain always returns true, meaning no filtering is performed.
func (f *NoOpFilter) MayContain(key []byte) bool {
	return true
}

// NewNoOpFilter creates a new no-op filter.
func NewNoOpFilter() Filter {
	return &NoOpFilter{}
}
