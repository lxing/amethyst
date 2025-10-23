package sstable

import "amethyst/internal/common"

// tableImpl is a placeholder implementation for future lessons.
type tableImpl struct{}

func (t *tableImpl) Get(key []byte) (value []byte, seq uint64, ok bool, err error) {
    panic("sstable: Get not implemented")
}

func (t *tableImpl) Path() string {
    panic("sstable: Path not implemented")
}

// buildTable is the internal helper students will eventually flesh out.
func buildTable(path string, entries []*common.Entry, indexInterval int) (Table, error) {
    panic("sstable: buildTable not implemented")
}
