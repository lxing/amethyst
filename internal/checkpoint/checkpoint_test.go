package checkpoint_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"amethyst/internal/checkpoint"
	"amethyst/internal/common"
	"amethyst/internal/db"

	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

func TestSaveRestoreFiles(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dbDir := filepath.Join(base, "adb")
	root := filepath.Join(base, "checkpoints")

	writeFile(t, filepath.Join(dbDir, "MANIFEST"), "v1")
	writeFile(t, filepath.Join(dbDir, "wal", "1.log"), "wal-v1")
	writeFile(t, filepath.Join(dbDir, "sstable", "0", "2.sst"), "sst")
	require.NoError(t, os.MkdirAll(filepath.Join(dbDir, "sstable", "1"), 0o755))

	info, err := checkpoint.Save(dbDir, root, "first")
	require.NoError(t, err)
	require.Equal(t, 3, info.Files)
	require.EqualValues(t, len("v1")+len("wal-v1")+len("sst"), info.Bytes)

	_, err = checkpoint.Save(dbDir, root, "first")
	require.Error(t, err, "existing checkpoint is not overwritten")

	// Mutate the DB after the checkpoint.
	writeFile(t, filepath.Join(dbDir, "MANIFEST"), "v2")
	writeFile(t, filepath.Join(dbDir, "wal", "1.log"), "wal-v2")
	writeFile(t, filepath.Join(dbDir, "sstable", "0", "3.sst"), "new")

	require.NoError(t, checkpoint.Restore(root, "first", dbDir))
	require.Equal(t, "v1", readFile(t, filepath.Join(dbDir, "MANIFEST")))
	require.Equal(t, "wal-v1", readFile(t, filepath.Join(dbDir, "wal", "1.log")))
	require.NoFileExists(t, filepath.Join(dbDir, "sstable", "0", "3.sst"))
	require.DirExists(t, filepath.Join(dbDir, "sstable", "1"), "empty dirs survive")

	// Writing to the restored DB must not leak back into the checkpoint.
	writeFile(t, filepath.Join(dbDir, "wal", "1.log"), "appended")
	require.Equal(t, "wal-v1", readFile(t, filepath.Join(root, "first", "wal", "1.log")))
	require.NoDirExists(t, dbDir+".restoring")
}

func TestListAndRemove(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dbDir := filepath.Join(base, "adb")
	root := filepath.Join(base, "checkpoints")
	writeFile(t, filepath.Join(dbDir, "MANIFEST"), "x")

	infos, err := checkpoint.List(root)
	require.NoError(t, err)
	require.Empty(t, infos, "missing root lists nothing")

	for _, name := range []string{"a", "b"} {
		_, err := checkpoint.Save(dbDir, root, name)
		require.NoError(t, err)
	}
	infos, err = checkpoint.List(root)
	require.NoError(t, err)
	require.Len(t, infos, 2)

	require.NoError(t, checkpoint.Remove(root, "a"))
	require.Error(t, checkpoint.Remove(root, "a"))
	infos, err = checkpoint.List(root)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, "b", infos[0].Name)
}

func TestRejectsBadNames(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	for _, name := range []string{"", ".", "..", "../escape", "a/b", ".hidden"} {
		_, err := checkpoint.Save(base, base, name)
		require.Error(t, err, name)
		require.Error(t, checkpoint.Restore(base, name, filepath.Join(base, "db")), name)
		require.Error(t, checkpoint.Remove(base, name), name)
	}
}

func TestRestoreMissingKeepsDB(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dbDir := filepath.Join(base, "adb")
	writeFile(t, filepath.Join(dbDir, "MANIFEST"), "keep")

	require.Error(t, checkpoint.Restore(filepath.Join(base, "checkpoints"), "nope", dbDir))
	require.Equal(t, "keep", readFile(t, filepath.Join(dbDir, "MANIFEST")))
}

// TestCheckpointRealDB saves a quiescent DB (memtable contents live only in
// the WAL, plus flushed SSTables), keeps writing, then restores and reopens.
func TestCheckpointRealDB(t *testing.T) {
	common.LoggingEnabled = false
	base := t.TempDir()
	dbDir := filepath.Join(base, "adb")
	root := filepath.Join(base, "checkpoints")

	d, err := db.Open(db.WithDBPath(dbDir), db.WithMemtableFlushThreshold(10))
	require.NoError(t, err)
	for i := range 25 { // two flushes + 5 entries left in the WAL
		require.NoError(t, d.Put(fmt.Appendf(nil, "k%02d", i), []byte("before")))
	}

	_, err = checkpoint.Save(dbDir, root, "snap")
	require.NoError(t, err)

	for i := range 40 {
		require.NoError(t, d.Put(fmt.Appendf(nil, "k%02d", i), []byte("after")))
	}
	require.NoError(t, d.Close())

	require.NoError(t, checkpoint.Restore(root, "snap", dbDir))
	d, err = db.Open(db.WithDBPath(dbDir), db.WithMemtableFlushThreshold(10))
	require.NoError(t, err)
	defer d.Close()

	for i := range 25 {
		v, err := d.Get(fmt.Appendf(nil, "k%02d", i))
		require.NoError(t, err, i)
		require.Equal(t, "before", string(v), i)
	}
	for i := 25; i < 40; i++ {
		_, err := d.Get(fmt.Appendf(nil, "k%02d", i))
		require.ErrorIs(t, err, db.ErrNotFound, i)
	}
}
