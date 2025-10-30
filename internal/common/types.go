package common

import (
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
	Seq   uint32
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
// │      keyLen      │  uint16 - len(key)
// ├──────────────────┤
// │       key        │  []byte
// ├──────────────────┤
// │     valueLen     │  uint16 - len(value), 0 for tombstones
// ├──────────────────┤
// │      value       │  []byte
// ├──────────────────┤
// │       seq        │  uint32
// ├──────────────────┤
// │    entryType     │  uint8 - 0=Put, 1=Delete
// └──────────────────┘

// WriteEntry writes an entry to the given writer.
// Returns the number of bytes written.
func WriteEntry(w io.Writer, e *Entry) (int, error) {
	total := 0

	// Write key_len (u16)
	n, err := WriteUint16(w, uint16(len(e.Key)))
	total += n
	if err != nil {
		return total, err
	}

	// Write key bytes
	if len(e.Key) > 0 {
		n, err = WriteBytes(w, e.Key)
		total += n
		if err != nil {
			return total, err
		}
	}

	// Write value_len (u16)
	n, err = WriteUint16(w, uint16(len(e.Value)))
	total += n
	if err != nil {
		return total, err
	}

	// Write value bytes
	if len(e.Value) > 0 {
		n, err = WriteBytes(w, e.Value)
		total += n
		if err != nil {
			return total, err
		}
	}

	// Write seq (u32)
	n, err = WriteUint32(w, e.Seq)
	total += n
	if err != nil {
		return total, err
	}

	// Write type (u8)
	n, err = WriteUint8(w, uint8(e.Type))
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// ReadEntry reads a single entry from the reader.
// Returns (nil, nil) when stream is exhausted (clean EOF).
// Returns (nil, ErrIncompleteEntry) for incomplete entries (malformed data).
func ReadEntry(r io.ByteReader) (*Entry, error) {
	reader := r.(io.Reader)

	// Read key_len (u16)
	keyLen, err := ReadUint16(reader)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, ErrIncompleteEntry
	}

	// Read key bytes
	key, err := ReadBytes(reader, uint64(keyLen))
	if err != nil {
		return nil, ErrIncompleteEntry
	}

	// Read value_len (u16)
	valueLen, err := ReadUint16(reader)
	if err != nil {
		return nil, ErrIncompleteEntry
	}

	// Read value bytes
	value, err := ReadBytes(reader, uint64(valueLen))
	if err != nil {
		return nil, ErrIncompleteEntry
	}

	// Read seq (u32)
	seq, err := ReadUint32(reader)
	if err != nil {
		return nil, ErrIncompleteEntry
	}

	// Read type (u8)
	entryType, err := ReadUint8(reader)
	if err != nil {
		return nil, ErrIncompleteEntry
	}

	return &Entry{
		Type:  EntryType(entryType),
		Seq:   seq,
		Key:   key,
		Value: value,
	}, nil
}
