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

// Entry encoding format:
//
//   entryType (uint8)   // 0 = Put, 1 = Delete (tombstone)
//   seq       (uint64)  // Sequence number (little-endian)
//   keyLen    (uint32)  // Length of key (little-endian)
//   valueLen  (uint32)  // Length of value (little-endian, 0 for tombstones)
//   key       ([]byte)  // Key bytes
//   value     ([]byte)  // Value bytes (omitted if valueLen = 0)

// Encode writes an entry to the given writer.
func (e *Entry) Encode(w io.Writer) error {
	var buf [1 + 8 + 4 + 4]byte

	buf[0] = byte(e.Type)
	binary.LittleEndian.PutUint64(buf[1:], e.Seq)
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[13:], uint32(len(e.Value)))

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	if len(e.Key) > 0 {
		if _, err := w.Write(e.Key); err != nil {
			return err
		}
	}

	if len(e.Value) > 0 {
		if _, err := w.Write(e.Value); err != nil {
			return err
		}
	}

	return nil
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

	// Read remaining 16 header bytes: seq(8) + keyLen(4) + valueLen(4)
	var hdr [16]byte
	for i := range hdr {
		b, err := r.ReadByte()
		if err != nil {
			return nil, ErrIncompleteEntry
		}
		hdr[i] = b
	}

	entry := &Entry{
		Type: EntryType(firstByte),
		Seq:  binary.LittleEndian.Uint64(hdr[0:8]),
	}

	keyLen := binary.LittleEndian.Uint32(hdr[8:12])
	valueLen := binary.LittleEndian.Uint32(hdr[12:16])

	// Read key
	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		if rdr, ok := r.(io.Reader); ok {
			// Fast path: read all at once
			if _, err := io.ReadFull(rdr, entry.Key); err != nil {
				return nil, ErrIncompleteEntry
			}
		} else {
			// Fallback: byte-by-byte
			for i := range entry.Key {
				b, err := r.ReadByte()
				if err != nil {
					return nil, ErrIncompleteEntry
				}
				entry.Key[i] = b
			}
		}
	}

	// Read value
	if valueLen > 0 {
		entry.Value = make([]byte, valueLen)
		if rdr, ok := r.(io.Reader); ok {
			// Fast path: read all at once
			if _, err := io.ReadFull(rdr, entry.Value); err != nil {
				return nil, ErrIncompleteEntry
			}
		} else {
			// Fallback: byte-by-byte
			for i := range entry.Value {
				b, err := r.ReadByte()
				if err != nil {
					return nil, ErrIncompleteEntry
				}
				entry.Value[i] = b
			}
		}
	}

	return entry, nil
}
