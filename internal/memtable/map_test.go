package memtable_test

import (
	"fmt"
	"testing"

	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"github.com/stretchr/testify/require"
)

func TestPutAndGet(t *testing.T) {
	mt := memtable.NewMapMemtable()

	require.Equal(t, 0, mt.Len())

	key := []byte("alpha")
	value := []byte("value")
	mt.Put(key, value, 1)

	require.Equal(t, 1, mt.Len())

	entry, ok := mt.Get([]byte("alpha"))
	require.True(t, ok)
	require.Equal(t, common.EntryTypePut, entry.Type)
	require.Equal(t, []byte("value"), entry.Value)
	require.Equal(t, uint32(1), entry.Seq)

	missing, ok := mt.Get([]byte("missing"))
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestOverwriteAndDeleteSameKey(t *testing.T) {
	mt := memtable.NewMapMemtable()

	key := []byte("duplicate")

	// Store an initial value, then overwrite it.
	mt.Put(key, []byte("v1"), 1)
	mt.Put(key, []byte("v2"), 2)

	entry, ok := mt.Get(key)
	require.True(t, ok)
	require.Equal(t, common.EntryTypePut, entry.Type)
	require.Equal(t, []byte("v2"), entry.Value)
	require.Equal(t, uint32(2), entry.Seq)

	// Place a tombstone for the key.
	mt.Delete(key, 3)
	entry, ok = mt.Get(key)
	require.True(t, ok)
	require.Equal(t, common.EntryTypeDelete, entry.Type)
	require.Equal(t, uint32(3), entry.Seq)

	// Writing after a tombstone acts like a fresh put.
	mt.Put(key, []byte("v3"), 4)
	entry, ok = mt.Get(key)
	require.True(t, ok)
	require.Equal(t, common.EntryTypePut, entry.Type)
	require.Equal(t, []byte("v3"), entry.Value)
	require.Equal(t, uint32(4), entry.Seq)
}

func TestBulkPutGetDelete(t *testing.T) {
	mt := memtable.NewMapMemtable()

	const n = 32
	expected := make(map[string]*common.Entry, 3*n)
	var nextSeq uint32

	// Write first n keys that will remain as puts.
	for i := 0; i < n; i++ {
		keyStr := fmt.Sprintf("key%d", i)
		key := []byte(keyStr)
		value := []byte(fmt.Sprintf("v%04d", i))
		nextSeq++
		mt.Put(key, value, nextSeq)
		expected[keyStr] = &common.Entry{
			Type:  common.EntryTypePut,
			Seq:   nextSeq,
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		}
	}

	// Write second n keys with _deleted suffix (these will be deleted).
	for i := n; i < 2*n; i++ {
		keyStr := fmt.Sprintf("key%d_deleted", i)
		key := []byte(keyStr)
		value := []byte(fmt.Sprintf("v%04d", i))
		nextSeq++
		mt.Put(key, value, nextSeq)
		expected[keyStr] = &common.Entry{
			Type:  common.EntryTypePut,
			Seq:   nextSeq,
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		}
	}

	// Verify all keys return their values before deletion.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, common.EntryTypePut, entry.Type)
		require.Equal(t, value, entry.Value)
	}
	for i := n; i < 2*n; i++ {
		key := []byte(fmt.Sprintf("key%d_deleted", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, common.EntryTypePut, entry.Type)
		require.Equal(t, value, entry.Value)
	}

	// Delete the second n keys (those with _deleted suffix).
	for i := n; i < 2*n; i++ {
		keyStr := fmt.Sprintf("key%d_deleted", i)
		key := []byte(keyStr)
		nextSeq++
		mt.Delete(key, nextSeq)
		expected[keyStr].Type = common.EntryTypeDelete
		expected[keyStr].Seq = nextSeq
		expected[keyStr].Value = nil
	}

	// Delete third n keys that were never written (direct tombstones).
	for i := 2 * n; i < 3*n; i++ {
		keyStr := fmt.Sprintf("key%d_never_existed", i)
		key := []byte(keyStr)
		nextSeq++
		mt.Delete(key, nextSeq)
		expected[keyStr] = &common.Entry{
			Type:  common.EntryTypeDelete,
			Seq:   nextSeq,
			Key:   append([]byte(nil), key...),
			Value: nil,
		}
	}

	// Confirm first n keys remain, second n keys are deleted, third n keys are deleted.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, common.EntryTypePut, entry.Type)
		require.Equal(t, []byte(fmt.Sprintf("v%04d", i)), entry.Value)
	}
	for i := n; i < 2*n; i++ {
		key := []byte(fmt.Sprintf("key%d_deleted", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, common.EntryTypeDelete, entry.Type)
	}
	for i := 2 * n; i < 3*n; i++ {
		key := []byte(fmt.Sprintf("key%d_never_existed", i))
		entry, ok := mt.Get(key)
		require.True(t, ok)
		require.Equal(t, common.EntryTypeDelete, entry.Type)
	}

	// Iterator must surface each mutation with the sequence/type/value we recorded.
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
		entryKey := string(entry.Key)
		expectation, ok := expected[entryKey]
		require.True(t, ok, "unexpected key: %s", entryKey)
		require.Equal(t, expectation.Seq, entry.Seq)
		require.Equal(t, expectation.Type, entry.Type)
		require.Equal(t, expectation.Value, entry.Value)
	}
	require.Equal(t, 3*n, count)
}
