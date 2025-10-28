package wal

import (
	"bufio"
	"errors"
	"os"

	"amethyst/internal/common"
)

// walImpl appends entries to a single file on disk.
type walImpl struct {
	file       *os.File
	entryCount int
}

var _ WAL = (*walImpl)(nil)

// countEntries counts the number of entries in a WAL file.
func countEntries(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	count := 0
	for {
		entry, err := common.ReadEntry(reader)
		if err != nil {
			return 0, err
		}
		if entry == nil {
			break
		}
		count++
	}
	return count, nil
}

// OpenWAL opens an existing WAL file for appending (used during recovery).
func OpenWAL(path string) (*walImpl, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	// Count existing entries in the file
	count, err := countEntries(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &walImpl{file: f, entryCount: count}, nil
}

// CreateWAL creates a new WAL file, truncating if it exists (used during rotation).
func CreateWAL(path string) (*walImpl, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &walImpl{file: f}, nil
}

// Close releases the underlying file handle.
func (l *walImpl) Close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// WriteEntry persists the provided batch. Entries are written sequentially.
func (l *walImpl) WriteEntry(batch []*common.Entry) error {
	if len(batch) == 0 {
		return nil
	}

	if l.file == nil {
		return errors.New("wal: log is closed")
	}

	for _, e := range batch {
		if _, err := common.WriteEntry(l.file, e); err != nil {
			return err
		}
	}
	l.entryCount += len(batch)
	return l.file.Sync()
}

// Iterator returns a streaming iterator over all log entries.
// The iterator will automatically close the underlying file when exhausted.
func (l *walImpl) Iterator() (common.EntryIterator, error) {
	f, err := os.Open(l.file.Name())
	if err != nil {
		return nil, err
	}

	return &walIterator{
		file:   f,
		reader: bufio.NewReader(f),
	}, nil
}

// Len returns the number of entries written to this WAL.
func (l *walImpl) Len() int {
	return l.entryCount
}

type walIterator struct {
	file   *os.File
	reader *bufio.Reader
}

var _ common.EntryIterator = (*walIterator)(nil)

func (it *walIterator) Next() (*common.Entry, error) {
	if it.file == nil {
		return nil, nil // Already closed
	}

	entry, err := common.ReadEntry(it.reader)
	if err != nil {
		// Error during decode - close and return error
		it.Close()
		return nil, err
	}

	if entry == nil {
		// Clean end of stream - close resources
		it.Close()
		return nil, nil
	}

	return entry, nil
}

// Close releases the underlying file handle.
// Safe to call multiple times.
func (it *walIterator) Close() error {
	if it.file == nil {
		return nil
	}
	err := it.file.Close()
	it.file = nil
	it.reader = nil
	return err
}
