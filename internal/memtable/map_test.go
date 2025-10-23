package memtable_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"github.com/stretchr/testify/require"
)

func TestPutAndGet(t *testing.T) {
	mt := memtable.NewMapMemtable()

	key := []byte("alpha")
	value := []byte("value")
	require.NoError(t, mt.Put(key, value))

	// Mutate original slices to ensure the memtable stored clones.
	key[0] = 'A'
	value[0] = 'V'

	stored, ok := mt.Get([]byte("alpha"))
	require.True(t, ok)
	require.Equal(t, []byte("value"), stored)

	// Mutating the returned slice should not affect stored state.
	stored[0] = 'V'
	again, ok := mt.Get([]byte("alpha"))
	require.True(t, ok)
	require.Equal(t, []byte("value"), again)

	// Ensure lookup with mutated key still returns data via fresh slice.
	missing, ok := mt.Get([]byte("Alpha"))
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestGetMissing(t *testing.T) {
	mt := memtable.NewMapMemtable()

	value, ok := mt.Get([]byte("missing"))
	require.False(t, ok)
	require.Nil(t, value)
}

func TestBulkPutGetDelete(t *testing.T) {
	mt := memtable.NewMapMemtable()

	const total = 512
	seqByKey := make(map[string]uint64)
	var nextSeq uint64
	for i := 0; i < total; i++ {
		key := makeIndexedKey(i)
		value := []byte(fmt.Sprintf("v%04d", i))
		require.NoError(t, mt.Put(key, value))
		nextSeq++
		seqByKey[string(key)] = nextSeq
	}

	for i := 0; i < total; i++ {
		key := makeIndexedKey(i)
		value := []byte(fmt.Sprintf("v%04d", i))
		stored, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, value, stored)
	}

	for i := 0; i < total; i += 2 {
		key := makeIndexedKey(i)
		require.NoError(t, mt.Delete(key))
		nextSeq++
		seqByKey[string(key)] = nextSeq
	}

	for i := 0; i < total; i++ {
		key := makeIndexedKey(i)
		stored, ok := mt.Get(key)
		if i%2 == 0 {
			require.False(t, ok)
			require.Nil(t, stored)
		} else {
			require.True(t, ok)
			require.Equal(t, []byte(fmt.Sprintf("v%04d", i)), stored)
		}
	}

	// Verify sequence numbers and tombstones via iterator.
	it := mt.Iterator()
	count := 0
	for {
		entry, err := it.Next()
		require.NoError(t, err)
		if entry == nil {
			break
		}
		count++
		require.NotNil(t, entry.Key)
		idx := decodeIndexedKey(entry.Key)
		require.GreaterOrEqual(t, idx, 0)
		if entry.Type == common.EntryTypeDelete {
			require.Equal(t, seqByKey[string(entry.Key)], entry.Seq)
			require.Nil(t, entry.Value)
		} else {
			require.Equal(t, seqByKey[string(entry.Key)], entry.Seq)
			require.Equal(t, []byte(fmt.Sprintf("v%04d", idx)), entry.Value)
		}
	}
	require.Equal(t, total, count)
}

func TestOverwriteAndDeleteSameKey(t *testing.T) {
	mt := memtable.NewMapMemtable()

	key := []byte("duplicate")

	require.NoError(t, mt.Put(key, []byte("v1")))
	require.NoError(t, mt.Put(key, []byte("v2")))

	stored, ok := mt.Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("v2"), stored)

	require.NoError(t, mt.Delete(key))
	stored, ok = mt.Get(key)
	require.False(t, ok)
	require.Nil(t, stored)

	it := mt.Iterator()
	entry, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, common.EntryTypeDelete, entry.Type)
	require.Equal(t, uint64(3), entry.Seq)
	require.Nil(t, entry.Value)

	none, err := it.Next()
	require.NoError(t, err)
	require.Nil(t, none)
}

func makeIndexedKey(index int) []byte {
	const suffix = "key"
	key := make([]byte, 2+len(suffix))
	binary.BigEndian.PutUint16(key[:2], uint16(index))
	copy(key[2:], suffix)
	return key
}

func decodeIndexedKey(key []byte) int {
	if len(key) < 2 {
		return -1
	}
	return int(binary.BigEndian.Uint16(key[:2]))
}
