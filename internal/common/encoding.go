package common

import (
	"encoding/binary"
	"io"
)

func WriteUint8(w io.Writer, v uint8) (int, error) {
	return w.Write([]byte{v})
}

func ReadUint8(r io.Reader) (uint8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func WriteUint64(w io.Writer, v uint64) (int, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return w.Write(buf[:])
}

func ReadUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func WriteBytes(w io.Writer, data []byte) (int, error) {
	return w.Write(data)
}

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
