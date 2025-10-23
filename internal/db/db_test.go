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

	// Create DB with low WAL threshold
	d, err := db.Open(db.WithWALThreshold(5))
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
