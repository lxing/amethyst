package wal

import (
	"bufio"
	"errors"
	"os"

	"amethyst/internal/common"
)

// WALImpl appends entries to a single file on disk.
type WALImpl struct {
	file       *os.File
	path       string
	entryCount int
}

// NewWAL creates (or reopens) a WAL file at path.
func NewWAL(path string) (*WALImpl, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WALImpl{file: f, path: path}, nil
}

// Close releases the underlying file handle.
func (l *WALImpl) Close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// WriteEntry persists the provided batch. Entries are written sequentially.
func (l *WALImpl) WriteEntry(batch []*common.Entry) error {
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
func (l *WALImpl) Iterator() (common.EntryIterator, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}

	return &walIterator{
		file:   f,
		reader: bufio.NewReader(f),
	}, nil
}

// Len returns the number of entries written to this WAL.
func (l *WALImpl) Len() int {
	return l.entryCount
}

type walIterator struct {
	file   *os.File
	reader *bufio.Reader
}

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
