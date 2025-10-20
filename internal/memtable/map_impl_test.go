package memtable_test

import (
	"testing"

	"amethyst/internal/memtable"
	"github.com/stretchr/testify/require"
)

func TestMapMemtablePutAndGet(t *testing.T) {
	mt := memtable.NewMemtable()

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

func TestMapMemtableDeleteMarksTombstone(t *testing.T) {
	mt := memtable.NewMemtable()

	require.NoError(t, mt.Delete(42, []byte("zeta")))

	entry, ok := mt.Get([]byte("zeta"))
	require.True(t, ok)
	require.True(t, entry.Tombstone)
	require.Equal(t, uint64(42), entry.Sequence)
	require.Nil(t, entry.Value)
}

func TestMapMemtableGetMissing(t *testing.T) {
	mt := memtable.NewMemtable()

	_, ok := mt.Get([]byte("missing"))
	require.False(t, ok)
}
