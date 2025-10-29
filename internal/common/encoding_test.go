package common

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadUint8(t *testing.T) {
	tests := []struct {
		name  string
		value uint8
	}{
		{"Zero", 0},
		{"One", 1},
		{"Max", 255},
		{"Mid", 128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := WriteUint8(&buf, tt.value)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			result, err := ReadUint8(&buf)
			require.NoError(t, err)
			require.Equal(t, tt.value, result)
		})
	}
}

func TestWriteReadUint32(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
	}{
		{"Zero", 0},
		{"One", 1},
		{"Max", 0xFFFFFFFF},
		{"Mid", 0x80000000},
		{"Large", 1234567890},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := WriteUint32(&buf, tt.value)
			require.NoError(t, err)
			require.Equal(t, 4, n)

			result, err := ReadUint32(&buf)
			require.NoError(t, err)
			require.Equal(t, tt.value, result)
		})
	}
}

func TestWriteReadBytes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", nil},
		{"SingleByte", []byte{0x42}},
		{"SmallData", []byte("hello")},
		{"LargeData", bytes.Repeat([]byte("x"), 1000)},
		{"BinaryData", []byte{0x00, 0xFF, 0x7F, 0x80}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := WriteBytes(&buf, tt.data)
			require.NoError(t, err)
			require.Equal(t, len(tt.data), n)

			result, err := ReadBytes(&buf, uint64(len(tt.data)))
			require.NoError(t, err)
			if len(tt.data) == 0 {
				require.Empty(t, result)
			} else {
				require.Equal(t, tt.data, result)
			}
		})
	}
}

func TestReadUint8Error(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	_, err := ReadUint8(buf)
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
}

func TestReadUint32Error(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Incomplete", []byte{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.data)
			_, err := ReadUint32(buf)
			require.Error(t, err)
		})
	}
}

func TestReadBytesError(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		length uint64
	}{
		{"EmptyButExpectingData", []byte{}, 5},
		{"IncompleteData", []byte{1, 2, 3}, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.data)
			_, err := ReadBytes(buf, tt.length)
			require.Error(t, err)
		})
	}
}

func TestReadBytesZeroLength(t *testing.T) {
	buf := bytes.NewBuffer([]byte{1, 2, 3})
	result, err := ReadBytes(buf, 0)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, 3, buf.Len())
}

func TestLittleEndianEncoding(t *testing.T) {
	var buf bytes.Buffer
	_, err := WriteUint32(&buf, 0x01020304)
	require.NoError(t, err)

	expected := []byte{0x04, 0x03, 0x02, 0x01}
	require.Equal(t, expected, buf.Bytes())
}
