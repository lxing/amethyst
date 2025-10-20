package memtable_test

import (
	"fmt"
	"testing"

	"amethyst/internal/memtable"
	"github.com/stretchr/testify/require"
)

func TestPutAndGet(t *testing.T) {
	mt := memtable.NewMapMemtable()

	key := []byte("alpha")
	value := []byte("value")
	require.NoError(t, mt.Put(1, key, value))

	// Mutate original slices to ensure the memtable stored clones.
	key[0] = 'A'
	value[0] = 'V'

	entry, ok := mt.Get([]byte("alpha"))
	require.True(t, ok)
	require.Equal(t, uint64(1), entry.Sequence)
	require.False(t, entry.Tombstone)
	require.Equal(t, []byte("value"), entry.Value)

	// Ensure lookup with mutated key still returns data via fresh slice.
	_, ok = mt.Get([]byte("Alpha"))
	require.False(t, ok)
}

func TestGetMissing(t *testing.T) {
	mt := memtable.NewMapMemtable()

	_, ok := mt.Get([]byte("missing"))
	require.False(t, ok)
}

func TestBulkPutGetDelete(t *testing.T) {
	mt := memtable.NewMapMemtable()

	const total = 512
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		require.NoError(t, mt.Put(uint64(i), key, value))
	}

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.False(t, entry.Tombstone)
		require.Equal(t, uint64(i), entry.Sequence)
		require.Equal(t, value, entry.Value)
	}

	for i := 0; i < total; i += 2 {
		key := []byte(fmt.Sprintf("k%04d", i))
		require.NoError(t, mt.Delete(uint64(total+i), key))
	}

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		if i%2 == 0 {
			require.True(t, entry.Tombstone)
			require.Equal(t, uint64(total+i), entry.Sequence)
		} else {
			require.False(t, entry.Tombstone)
			require.Equal(t, uint64(i), entry.Sequence)
			require.Equal(t, []byte(fmt.Sprintf("v%04d", i)), entry.Value)
		}
	}
}

func TestOverwriteAndDeleteSameKey(t *testing.T) {
	mt := memtable.NewMapMemtable()

	key := []byte("duplicate")

	require.NoError(t, mt.Put(1, key, []byte("v1")))
	require.NoError(t, mt.Put(2, key, []byte("v2")))

	entry, ok := mt.Get(key)
	require.True(t, ok)
	require.False(t, entry.Tombstone)
	require.Equal(t, uint64(2), entry.Sequence)
	require.Equal(t, []byte("v2"), entry.Value)

	require.NoError(t, mt.Delete(3, key))
	entry, ok = mt.Get(key)
	require.True(t, ok)
	require.True(t, entry.Tombstone)
	require.Equal(t, uint64(3), entry.Sequence)
}
