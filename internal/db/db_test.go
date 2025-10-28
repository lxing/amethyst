package db_test

import (
	"fmt"
	"os"
	"testing"

	"amethyst/internal/db"
	"github.com/stretchr/testify/require"
)

func TestWALRotation(t *testing.T) {
	// Clean up
	defer os.RemoveAll("wal")
	defer os.RemoveAll("sstable")
	defer os.Remove("MANIFEST")
	defer os.Remove("MANIFEST.tmp")

	// Create DB with low memtable flush threshold
	d, err := db.Open(db.WithMemtableFlushThreshold(5))
	require.NoError(t, err)

	// Write 4 entries (below threshold)
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := d.Put(key, value)
		require.NoError(t, err)
	}

	// Verify we can read them
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value, err := d.Get(key)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
	}

	// Write 5th entry - reaches threshold exactly but doesn't exceed yet
	err = d.Put([]byte("key4"), []byte("value4"))
	require.NoError(t, err)

	// Write 6th entry - now exceeds threshold, triggers rotation
	err = d.Put([]byte("key_trigger"), []byte("value_trigger"))
	require.NoError(t, err)

	// Verify new WAL file was created (wal/1.log)
	_, err = os.Stat("wal/1.log")
	require.NoError(t, err, "WAL rotation should create wal/1.log")

	// Write one more entry to new WAL
	err = d.Put([]byte("key5"), []byte("value5"))
	require.NoError(t, err)

	// Verify we can read it from new memtable
	value, err := d.Get([]byte("key5"))
	require.NoError(t, err)
	require.Equal(t, []byte("value5"), value)
}

func TestSSTableReadAfterFlush(t *testing.T) {
	// Clean up
	defer os.RemoveAll("wal")
	defer os.RemoveAll("sstable")
	defer os.Remove("MANIFEST")
	defer os.Remove("MANIFEST.tmp")

	// Create DB with low memtable flush threshold to trigger flush
	d, err := db.Open(db.WithMemtableFlushThreshold(3))
	require.NoError(t, err)

	// Write 3 entries (reaches threshold)
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("old%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := d.Put(key, value)
		require.NoError(t, err)
	}

	// Write 4th entry - triggers flush to SSTable
	err = d.Put([]byte("trigger"), []byte("flush"))
	require.NoError(t, err)

	// Verify SSTable file was created
	_, err = os.Stat("sstable/0/0.sst")
	require.NoError(t, err, "Flush should create sstable/0/0.sst")

	// Write new entry to new memtable
	err = d.Put([]byte("new"), []byte("value"))
	require.NoError(t, err)

	// Read from new memtable (should work)
	value, err := d.Get([]byte("new"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)

	// Read from flushed SSTable (old entries)
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("old%d", i))
		value, err := d.Get(key)
		require.NoError(t, err, "Should read old%d from SSTable", i)
		require.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
	}

	// Read trigger key from SSTable
	value, err = d.Get([]byte("trigger"))
	require.NoError(t, err)
	require.Equal(t, []byte("flush"), value)

	// Verify non-existent key returns ErrNotFound
	_, err = d.Get([]byte("nonexistent"))
	require.ErrorIs(t, err, db.ErrNotFound)
}

func TestSSTableWithDeletes(t *testing.T) {
	// Clean up
	defer os.RemoveAll("wal")
	defer os.RemoveAll("sstable")
	defer os.Remove("MANIFEST")
	defer os.Remove("MANIFEST.tmp")

	// Create DB
	d, err := db.Open(db.WithMemtableFlushThreshold(5))
	require.NoError(t, err)

	// Write and delete in same memtable
	err = d.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = d.Delete([]byte("key1"))
	require.NoError(t, err)

	// Should return ErrNotFound
	_, err = d.Get([]byte("key1"))
	require.ErrorIs(t, err, db.ErrNotFound)

	// Write enough to trigger flush
	for i := 0; i < 5; i++ {
		err = d.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	// Deleted key should still be not found after flush
	_, err = d.Get([]byte("key1"))
	require.ErrorIs(t, err, db.ErrNotFound)

	// Other keys should be readable
	value, err := d.Get([]byte("k0"))
	require.NoError(t, err)
	require.Equal(t, []byte("v0"), value)
}
