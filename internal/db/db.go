package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"amethyst/internal/common"
	"amethyst/internal/manifest"
	"amethyst/internal/memtable"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

var ErrNotFound = errors.New("key not found")

type DB struct {
	mu        sync.RWMutex
	nextSeq   uint32
	memtable  memtable.Memtable
	wal       wal.WAL
	manifest  *manifest.Manifest
	Opts      Options
	paths     *common.PathManager
	writeChan chan *writeRequest
}

func Open(optFns ...Option) (*DB, error) {
	opts := DefaultOptions
	for _, fn := range optFns {
		fn(&opts)
	}

	paths := common.NewPathManager(opts.DBPath)

	// Create directories
	if err := os.MkdirAll(paths.WALDir(), 0755); err != nil {
		return nil, err
	}
	for i := 0; i <= opts.MaxSSTableLevel; i++ {
		sstableDir := fmt.Sprintf("%s/%d", paths.SSTableDir(), i)
		if err := os.MkdirAll(sstableDir, 0755); err != nil {
			return nil, err
		}
	}

	// Try to load existing manifest
	var m *manifest.Manifest
	var log wal.WAL
	var mt memtable.Memtable
	var nextSeq uint32

	manifestPath := paths.ManifestPath()
	if manifestFile, err := os.Open(manifestPath); err == nil {
		// Recovery path: manifest exists
		defer manifestFile.Close()

		version, err := manifest.ReadManifest(manifestFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest: %w", err)
		}

		m = manifest.NewManifest(paths, opts.MaxSSTableLevel+1)
		m.LoadVersion(version)

		// Open existing WAL for recovery
		walPath := paths.WALPath(version.CurrentWAL)
		log, err = wal.OpenWAL(walPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL: %w", err)
		}

		// Replay WAL into memtable
		mt = memtable.NewMapMemtable()
		nextSeq, err = replayWAL(log, mt)
		if err != nil {
			log.Close()
			return nil, fmt.Errorf("failed to replay WAL: %w", err)
		}

		common.Logf("recovered from manifest: wal=%d seq=%d\n", version.CurrentWAL, nextSeq)
	} else {
		// Fresh DB path: no manifest
		m = manifest.NewManifest(paths, opts.MaxSSTableLevel+1)

		// Create initial WAL
		walPath := paths.WALPath(m.Current().NextWALNumber)
		log, err = wal.CreateWAL(walPath)
		if err != nil {
			return nil, err
		}

		m.SetWAL(m.Current().NextWALNumber)

		// Persist initial manifest to disk
		if err = m.Flush(); err != nil {
			return nil, fmt.Errorf("failed to write initial manifest: %w", err)
		}

		mt = memtable.NewMapMemtable()
		nextSeq = 0
	}

	db := &DB{
		nextSeq:   nextSeq,
		memtable:  mt,
		wal:       log,
		manifest:  m,
		Opts:      opts,
		paths:     paths,
		writeChan: make(chan *writeRequest, 100),
	}

	// Start background group commit loop
	go db.groupCommitLoop()

	return db, nil
}

// replayWAL replays all entries from the WAL into the memtable.
// Returns the highest sequence number seen.
func replayWAL(w wal.WAL, mt memtable.Memtable) (uint32, error) {
	iter, err := w.Iterator()
	if err != nil {
		return 0, err
	}

	var maxSeq uint32
	for {
		entry, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if entry == nil {
			break
		}

		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}

		switch entry.Type {
		case common.EntryTypePut:
			mt.Put(entry.Key, entry.Value)
		case common.EntryTypeDelete:
			mt.Delete(entry.Key)
		}
	}

	return maxSeq, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	entry := &common.Entry{
		Type:  common.EntryTypePut,
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
		// Seq assigned by group commit loop
	}

	req := &writeRequest{
		entry:    entry,
		resultCh: make(chan error, 1),
	}

	d.writeChan <- req
	return <-req.resultCh
}

func (d *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	entry := &common.Entry{
		Type: common.EntryTypeDelete,
		Key:  bytes.Clone(key),
		// Seq assigned by group commit loop
	}

	req := &writeRequest{
		entry:    entry,
		resultCh: make(chan error, 1),
	}

	d.writeChan <- req
	return <-req.resultCh
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	common.Logf("get key=%q\n", string(key))
	common.Logf("  checking memtable\n")
	entry, ok := d.memtable.Get(key)
	if ok {
		if entry.Type == common.EntryTypeDelete {
			common.Logf("  found tombstone in memtable\n")
			return nil, ErrNotFound
		}
		common.Logf("  found in memtable\n")
		return bytes.Clone(entry.Value), nil
	}

	version := d.manifest.Current()
	for level, fileMetas := range version.Levels {
		common.Logf("  checking L%d (%d files)\n", level, len(fileMetas))

		// L0 has overlapping ranges, check newest to oldest
		// L1+ are non-overlapping, order doesn't matter (for now)
		files := fileMetas
		if level == 0 {
			// Reverse iteration for L0 to check newest files first
			files = make([]manifest.FileMetadata, len(fileMetas))
			for i, fm := range fileMetas {
				files[len(fileMetas)-1-i] = fm
			}
		}

		// TODO: Optimize lookup for L1+
		// L0 files have overlapping ranges, so we must check all files.
		// L1+ files are non-overlapping within a level, so we can binary search
		// by key range to find the single file that might contain the key.
		for _, fm := range files {
			table, err := d.manifest.GetTable(fm.FileNo, level)
			if err != nil {
				continue
			}

			entry, err := table.Get(key)
			if err == sstable.ErrNotFound {
				common.Logf("    not in L%d/%d.sst\n", level, fm.FileNo)
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read from L%d/%d.sst: %w", level, fm.FileNo, err)
			}

			if entry.Type == common.EntryTypeDelete {
				common.Logf("    found tombstone in L%d/%d.sst\n", level, fm.FileNo)
				return nil, ErrNotFound
			}
			common.Logf("    found in L%d/%d.sst\n", level, fm.FileNo)
			return bytes.Clone(entry.Value), nil
		}
	}

	common.Logf("get key=%q: not found\n", string(key))
	return nil, ErrNotFound
}

// flushMemtable writes the current memtable to an SSTable and rotates the WAL.
// Must be called with d.mu held.
func (d *DB) flushMemtable() error {
	v := d.manifest.Current()
	newWALNum := v.NextWALNumber

	// 1. Close old WAL (no more writes needed)
	d.wal.Close()

	// 2. Create new WAL file
	newWALPath := d.paths.WALPath(newWALNum)
	newWAL, err := wal.CreateWAL(newWALPath)
	if err != nil {
		return err
	}

	// 3. Write memtable to SSTable
	if err := d.writeSSTable(); err != nil {
		return err
	}

	// 4. Update manifest (atomic commit point)
	d.manifest.SetWAL(newWALNum)

	// 5. Persist manifest to disk (makes new files visible)
	if err := d.manifest.Flush(); err != nil {
		return err
	}

	// 6. Swap to new WAL and new memtable
	d.wal = newWAL
	d.memtable = memtable.NewMapMemtable()

	return nil
}

// writeSSTable writes the current memtable to an SSTable file.
// Must be called with d.mu held.
func (d *DB) writeSSTable() error {
	start := time.Now()

	// Get next SSTable number from manifest
	v := d.manifest.Current()
	fileNo := v.NextSSTableNumber

	// Create SSTable file in L0
	path := d.paths.SSTablePath(0, fileNo)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}

	// Get sorted entries from memtable
	iter := d.memtable.Iterator()

	// Write all entries to SSTable
	result, err := sstable.WriteSSTable(f, iter)
	if err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// Update manifest to add new SSTable to L0
	edit := &manifest.CompactionEdit{
		AddSSTables: map[int][]manifest.FileMetadata{
			0: {
				{
					FileNo:      fileNo,
					SmallestKey: result.SmallestKey,
					LargestKey:  result.LargestKey,
				},
			},
		},
	}
	d.manifest.Apply(edit)

	common.LogDuration(start, "flushed %d entries to %d.sst", result.EntryCount, fileNo)
	return nil
}

func (d *DB) Memtable() memtable.Memtable {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memtable
}

func (d *DB) WAL() wal.WAL {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.wal
}

func (d *DB) Manifest() *manifest.Manifest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.manifest
}

func (d *DB) Paths() *common.PathManager {
	return d.paths
}

// Close stops all database operations and releases resources.
// Currently a stub for future cleanup (closing WAL, flushing buffers, etc.)
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// TODO: Close WAL
	// TODO: Close manifest (which closes table cache)
	// TODO: Flush any pending writes

	return nil
}

