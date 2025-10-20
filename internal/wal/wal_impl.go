package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// WALImpl appends entries to a single file on disk.
type WALImpl struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// OpenWAL creates (or reopens) a WAL file at path.
func OpenWAL(path string) (*WALImpl, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WALImpl{
		file: f,
		path: path,
	}, nil
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
func (l *WALImpl) Append(ctx context.Context, batch []Entry) error {
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
		if err := ctx.Err(); err != nil {
			return err
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
	return l.file.Sync()
}

// Iterator returns a forward-only reader over all log entries.
func (l *WALImpl) Iterator(ctx context.Context) (WALIterator, error) {
	r, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	return &fileIterator{
		ctx: ctx,
		f:   r,
		br:  bufio.NewReader(r),
	}, nil
}

type fileIterator struct {
	ctx context.Context
	f   *os.File
	br  *bufio.Reader
}

func (it *fileIterator) Next() (Entry, bool, error) {
	if err := it.ctx.Err(); err != nil {
		return Entry{}, false, err
	}

	b, err := it.br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}

	var seqBuf [8]byte
	if _, err := io.ReadFull(it.br, seqBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}

	keyLen, err := binary.ReadUvarint(it.br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}

	valLen, err := binary.ReadUvarint(it.br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}

	entry := Entry{
		Type: EntryType(b),
		Seq:  binary.LittleEndian.Uint64(seqBuf[:]),
	}

	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(it.br, entry.Key); err != nil {
			return Entry{}, false, err
		}
	}
	if valLen > 0 {
		entry.Value = make([]byte, valLen)
		if _, err := io.ReadFull(it.br, entry.Value); err != nil {
			return Entry{}, false, err
		}
	}

	return entry, true, nil
}

func (it *fileIterator) Close() error {
	return it.f.Close()
}
