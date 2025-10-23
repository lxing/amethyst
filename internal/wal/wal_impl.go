package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"amethyst/internal/common"
)

// WALImpl appends entries to a single file on disk.
type WALImpl struct {
	mu      sync.Mutex
	file    *os.File
	path    string
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Append persists the provided batch. Entries are written sequentially.
func (l *WALImpl) Append(batch []*common.Entry) error {
	if len(batch) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return errors.New("wal: log is closed")
	}

	var hdr [1 + 8]byte
	var varintBuf [binary.MaxVarintLen64]byte

	for _, e := range batch {
		if e == nil {
			return errors.New("wal: nil entry")
		}
		hdr[0] = byte(e.Type)
		binary.LittleEndian.PutUint64(hdr[1:], e.Seq)
		if _, err := l.file.Write(hdr[:]); err != nil {
			return err
		}
		n := binary.PutUvarint(varintBuf[:], uint64(len(e.Key)))
		if _, err := l.file.Write(varintBuf[:n]); err != nil {
			return err
		}
		n = binary.PutUvarint(varintBuf[:], uint64(len(e.Value)))
		if _, err := l.file.Write(varintBuf[:n]); err != nil {
			return err
		}
		if len(e.Key) > 0 {
			if _, err := l.file.Write(e.Key); err != nil {
				return err
			}
		}
		if len(e.Value) > 0 {
			if _, err := l.file.Write(e.Value); err != nil {
				return err
			}
		}
	}
	l.entryCount += len(batch)
	return l.file.Sync()
}

// Iterator returns an in-memory iterator over all log entries.
func (l *WALImpl) Iterator() (common.EntryIterator, error) {
	r, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	br := bufio.NewReader(r)
	entries := make([]*common.Entry, 0)
	for {
		entry, err := readEntry(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}

	return &walIterator{entries: entries}, nil
}

// Len returns the number of entries written to this WAL.
func (l *WALImpl) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entryCount
}

type walIterator struct {
	entries []*common.Entry
	index   int
}

func (it *walIterator) Next() (*common.Entry, error) {
	if it.index >= len(it.entries) {
		return nil, nil
	}
	entry := it.entries[it.index]
	it.index++
	return entry, nil
}

func readEntry(br *bufio.Reader) (*common.Entry, error) {
	b, err := br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	var seqBuf [8]byte
	if _, err := io.ReadFull(br, seqBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	keyLen, err := binary.ReadUvarint(br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	valLen, err := binary.ReadUvarint(br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	entry := &common.Entry{
		Type: common.EntryType(b),
		Seq:  binary.LittleEndian.Uint64(seqBuf[:]),
	}

	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(br, entry.Key); err != nil {
			return nil, err
		}
	}
	if valLen > 0 {
		entry.Value = make([]byte, valLen)
		if _, err := io.ReadFull(br, entry.Value); err != nil {
			return nil, err
		}
	}

	return entry, nil
}
