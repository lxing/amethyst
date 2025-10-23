package sstable

import "amethyst/internal/common"

// Table exposes read access to an immutable SSTable.
type Table interface {
    Get(key []byte) (value []byte, seq uint64, ok bool, err error)
    Path() string
}

// Build will be implemented in sstable_impl.go once students flush memtables to disk.
func Build(path string, entries []*common.Entry, indexInterval int) (Table, error) {
    panic("sstable: Build not implemented")
}
