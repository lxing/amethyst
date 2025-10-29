package common

import (
	"encoding/binary"
	"io"
)

// WriteUint8 writes a single byte to the writer.
// Returns the number of bytes written (always 1) and any error encountered.
func WriteUint8(w io.Writer, v uint8) (int, error) {
	return w.Write([]byte{v})
}

// ReadUint8 reads a single byte from the reader.
// Returns the byte value and any error encountered.
func ReadUint8(r io.Reader) (uint8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// WriteUint32 writes a 32-bit unsigned integer in little-endian format.
// Returns the number of bytes written (always 4) and any error encountered.
func WriteUint32(w io.Writer, v uint32) (int, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return w.Write(buf[:])
}

// ReadUint32 reads a 32-bit unsigned integer in little-endian format.
// Returns the integer value and any error encountered.
func ReadUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

// WriteBytes writes raw bytes to the writer without any length prefix.
// Returns the number of bytes written and any error encountered.
func WriteBytes(w io.Writer, data []byte) (int, error) {
	return w.Write(data)
}

// ReadBytes reads exactly length bytes from the reader.
// Returns nil for length 0, otherwise returns a byte slice of the requested length.
// Returns any error encountered during reading.
func ReadBytes(r io.Reader, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
