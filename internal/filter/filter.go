package filter

// noOpFilter is a filter that always returns true (no filtering).
// Used as a placeholder until bloom filter is implemented.
type noOpFilter struct{}

var _ Filter = (*noOpFilter)(nil)

// MayContain always returns true, meaning no filtering is performed.
func (f *noOpFilter) MayContain(key []byte) bool {
	return true
}

// NewnoOpFilter creates a new no-op filter.
func NewnoOpFilter() Filter {
	return &noOpFilter{}
}
