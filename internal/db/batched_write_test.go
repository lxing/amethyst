package db_test

import (
	"fmt"
	"testing"

	"amethyst/internal/db"
	"github.com/stretchr/testify/require"
)

func TestConcurrentWrites(t *testing.T) {
	defer cleanupDB(t)

	// Create DB with higher threshold to avoid flushes during test
	d, err := db.Open(db.WithMemtableFlushThreshold(1000))
	require.NoError(t, err)

	// Number of concurrent writers and writes per writer
	numWriters := 10
	writesPerWriter := 100

	// Channel to signal completion
	done := make(chan bool, numWriters)

	// Launch concurrent writers
	for w := 0; w < numWriters; w++ {
		writerID := w
		go func() {
			for i := 0; i < writesPerWriter; i++ {
				key := []byte(fmt.Sprintf("writer%d_key%d", writerID, i))
				value := []byte(fmt.Sprintf("value%d", i))
				err := d.Put(key, value)
				require.NoError(t, err)
			}
			done <- true
		}()
	}

	// Wait for all writers to complete
	for w := 0; w < numWriters; w++ {
		<-done
	}

	// Verify all writes succeeded
	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			key := []byte(fmt.Sprintf("writer%d_key%d", w, i))
			value, err := d.Get(key)
			require.NoError(t, err, "Should find key %s", string(key))
			require.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
		}
	}
}

func TestConcurrentWritesWithFlush(t *testing.T) {
	defer cleanupDB(t)

	// Create DB with low threshold to trigger flushes during concurrent writes
	d, err := db.Open(db.WithMemtableFlushThreshold(50))
	require.NoError(t, err)

	// Number of concurrent writers and writes per writer
	numWriters := 5
	writesPerWriter := 100

	// Channel to signal completion
	done := make(chan bool, numWriters)

	// Launch concurrent writers
	for w := 0; w < numWriters; w++ {
		writerID := w
		go func() {
			for i := 0; i < writesPerWriter; i++ {
				key := []byte(fmt.Sprintf("w%d_k%d", writerID, i))
				value := []byte(fmt.Sprintf("v%d", i))
				err := d.Put(key, value)
				require.NoError(t, err)
			}
			done <- true
		}()
	}

	// Wait for all writers to complete
	for w := 0; w < numWriters; w++ {
		<-done
	}

	// Verify all writes succeeded (some in memtable, some in SSTables)
	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			key := []byte(fmt.Sprintf("w%d_k%d", w, i))
			value, err := d.Get(key)
			require.NoError(t, err, "Should find key %s", string(key))
			require.Equal(t, []byte(fmt.Sprintf("v%d", i)), value)
		}
	}
}
