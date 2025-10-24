package common

import (
	"encoding/binary"
	"io"
)

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

// Entry captures a single mutation in sequence order.
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

// Encode writes an entry to the given writer.
// Format: type(1) + seq(8) + keyLen(varint) + valueLen(varint) + key + value
func (e *Entry) Encode(w io.Writer) error {
	var hdr [1 + 8]byte
	var varintBuf [binary.MaxVarintLen64]byte

	hdr[0] = byte(e.Type)
	binary.LittleEndian.PutUint64(hdr[1:], e.Seq)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	n := binary.PutUvarint(varintBuf[:], uint64(len(e.Key)))
	if _, err := w.Write(varintBuf[:n]); err != nil {
		return err
	}

	n = binary.PutUvarint(varintBuf[:], uint64(len(e.Value)))
	if _, err := w.Write(varintBuf[:n]); err != nil {
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
// Returns nil entry on EOF. Returns error on malformed data.
func DecodeEntry(r io.ByteReader) (*Entry, error) {
	// Read type (1 byte)
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read seq (8 bytes)
	var seqBuf [8]byte
	for i := 0; i < 8; i++ {
		seqBuf[i], err = r.ReadByte()
		if err != nil {
			return nil, err
		}
	}

	// Read keyLen (varint)
	keyLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	// Read valueLen (varint)
	valueLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	entry := &Entry{
		Type: EntryType(typeByte),
		Seq:  binary.LittleEndian.Uint64(seqBuf[:]),
	}

	// Read key
	if keyLen > 0 {
		entry.Key = make([]byte, keyLen)
		for i := range entry.Key {
			entry.Key[i], err = r.ReadByte()
			if err != nil {
				return nil, err
			}
		}
	}

	// Read value
	if valueLen > 0 {
		entry.Value = make([]byte, valueLen)
		for i := range entry.Value {
			entry.Value[i], err = r.ReadByte()
			if err != nil {
				return nil, err
			}
		}
	}

	return entry, nil
}
