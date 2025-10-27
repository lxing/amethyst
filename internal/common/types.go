package common

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrIncompleteEntry = errors.New("incomplete entry: unexpected end of data")

// FileNo identifies a file (SSTable or WAL).
type FileNo uint64

// BlockNo identifies a block within an SSTable.
type BlockNo int

// EntryType enumerates logical operations flowing through WAL, memtable,
// and SSTable components.
type EntryType uint8

const (
	EntryTypePut EntryType = iota
	EntryTypeDelete
)

// Entry represents a single key-value pair in the database.
// It supports serialization and deserialization to/from a byte stream.
type Entry struct {
	Type  EntryType
	Seq   uint64
	Key   []byte
	Value []byte
}

// EntryIterator produces a stream of entries. Next returns nil when the stream
// is exhausted. Implementations should close underlying resources separately.
type EntryIterator interface {
	Next() (*Entry, error)
}

// Entry Layout:
//
// ┌──────────────────┐
// │    entryType     │  uint8 - 0=Put, 1=Delete
// ├──────────────────┤
// │       seq        │  uint64
// ├──────────────────┤
// │      keyLen      │  uint64 - len(key)
// ├──────────────────┤
// │     valueLen     │  uint64 - len(value), 0 for tombstones
// ├──────────────────┤
// │       key        │  []byte
// ├──────────────────┤
// │      value       │  []byte
// └──────────────────┘

// Encode writes an entry to the given writer.
// Returns the number of bytes written.
func (e *Entry) Encode(w io.Writer) (int, error) {
	var buf [1 + 8 + 8 + 8]byte

	buf[0] = byte(e.Type)
	binary.LittleEndian.PutUint64(buf[1:], e.Seq)
	binary.LittleEndian.PutUint64(buf[9:], uint64(len(e.Key)))
	binary.LittleEndian.PutUint64(buf[17:], uint64(len(e.Value)))

	n, err := w.Write(buf[:])
	if err != nil {
		return n, err
	}
	total := n

	if len(e.Key) > 0 {
		n, err := w.Write(e.Key)
		total += n
		if err != nil {
			return total, err
		}
	}

	if len(e.Value) > 0 {
		n, err := w.Write(e.Value)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// DecodeEntry reads a single entry from the reader.
// Returns (nil, nil) when stream is exhausted (clean EOF).
// Returns (nil, ErrIncompleteEntry) for incomplete entries (malformed data).
func DecodeEntry(r io.ByteReader) (*Entry, error) {
	// Try to read first byte - EOF here means clean end of stream
	firstByte, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, nil // Clean end of stream
		}
		return nil, err
	}

	// Read remaining 24 header bytes: seq(8) + keyLen(8) + valueLen(8)
	var hdr [24]byte
	if _, err := io.ReadFull(r.(io.Reader), hdr[:]); err != nil {
		return nil, ErrIncompleteEntry
	}

	entry := &Entry{
		Type: EntryType(firstByte),
		Seq:  binary.LittleEndian.Uint64(hdr[0:8]),
	}

	keyLen := binary.LittleEndian.Uint64(hdr[8:16])
	valueLen := binary.LittleEndian.Uint64(hdr[16:24])

	// Read key
	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r.(io.Reader), entry.Key); err != nil {
			return nil, ErrIncompleteEntry
		}
	}

	// Read value
	if valueLen > 0 {
		entry.Value = make([]byte, valueLen)
		if _, err := io.ReadFull(r.(io.Reader), entry.Value); err != nil {
			return nil, ErrIncompleteEntry
		}
	}

	return entry, nil
}
