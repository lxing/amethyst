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

type Options struct {
	MemtableFlushThreshold int
	MaxSSTableLevel        int
}

var DefaultOptions = Options{
	MemtableFlushThreshold: 256,
	MaxSSTableLevel:        3,
}

type Option func(*Options)

func WithMemtableFlushThreshold(n int) Option {
	return func(o *Options) {
		o.MemtableFlushThreshold = n
	}
}

func WithMaxSSTableLevel(n int) Option {
	return func(o *Options) {
		o.MaxSSTableLevel = n
	}
}

type DB struct {
	mu       sync.RWMutex
	nextSeq  uint64
	memtable memtable.Memtable
	wal      wal.WAL
	manifest *manifest.Manifest
	Opts     Options
}

func Open(optFns ...Option) (*DB, error) {
	opts := DefaultOptions
	for _, fn := range optFns {
		fn(&opts)
	}

	// Create directories
	if err := os.MkdirAll("wal", 0755); err != nil {
		return nil, err
	}
	for i := 0; i <= opts.MaxSSTableLevel; i++ {
		if err := os.MkdirAll(fmt.Sprintf("sstable/%d", i), 0755); err != nil {
			return nil, err
		}
	}

	// Try to load existing manifest
	var m *manifest.Manifest
	var log wal.WAL
	var mt memtable.Memtable
	var nextSeq uint64

	if manifestFile, err := os.Open("MANIFEST"); err == nil {
		// Recovery path: manifest exists
		defer manifestFile.Close()

		version, err := manifest.ReadManifest(manifestFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest: %w", err)
		}

		m = manifest.NewManifest(opts.MaxSSTableLevel + 1)
		m.LoadVersion(version)

		// Open existing WAL for recovery
		walPath := common.WALPath(version.CurrentWAL)
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
		m = manifest.NewManifest(opts.MaxSSTableLevel + 1)

		// Create initial WAL
		walPath := common.WALPath(m.Current().NextWALNumber)
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

	return &DB{
		nextSeq:  nextSeq,
		memtable: mt,
		wal:      log,
		manifest: m,
		Opts:     opts,
	}, nil
}

// replayWAL replays all entries from the WAL into the memtable.
// Returns the highest sequence number seen.
func replayWAL(w wal.WAL, mt memtable.Memtable) (uint64, error) {
	iter, err := w.Iterator()
	if err != nil {
		return 0, err
	}

	var maxSeq uint64
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

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if we need to flush memtable
	if d.memtable.Len() >= d.Opts.MemtableFlushThreshold {
		if err := d.flushMemtable(); err != nil {
			return err
		}
	}

	d.nextSeq++

	entry := &common.Entry{
		Type:  common.EntryTypePut,
		Seq:   d.nextSeq,
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
	}
	if err := d.wal.WriteEntry([]*common.Entry{entry}); err != nil {
		return err
	}

	d.memtable.Put(entry.Key, entry.Value)
	return nil
}

func (d *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if we need to flush memtable
	if d.memtable.Len() >= d.Opts.MemtableFlushThreshold {
		if err := d.flushMemtable(); err != nil {
			return err
		}
	}

	d.nextSeq++

	entry := &common.Entry{
		Type: common.EntryTypeDelete,
		Seq:  d.nextSeq,
		Key:  bytes.Clone(key),
	}
	if err := d.wal.WriteEntry([]*common.Entry{entry}); err != nil {
		return err
	}

	d.memtable.Delete(entry.Key)
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	common.Logf("get key=%q: checking memtable\n", string(key))
	entry, ok := d.memtable.Get(key)
	if ok {
		if entry.Type == common.EntryTypeDelete {
			common.Logf("get key=%q: found tombstone in memtable\n", string(key))
			return nil, ErrNotFound
		}
		common.Logf("get key=%q: found in memtable\n", string(key))
		return bytes.Clone(entry.Value), nil
	}

	version := d.manifest.Current()
	for level, fileNos := range version.Levels {
		common.Logf("get key=%q: checking L%d (%d files)\n", string(key), level, len(fileNos))
		// TODO: Optimize lookup for L1+
		// L0 files have overlapping ranges, so we must check all files.
		// L1+ files are non-overlapping within a level, so we can binary search
		// by key range to find the single file that might contain the key.
		for _, fileNo := range fileNos {
			table, err := d.manifest.GetTable(fileNo, level)
			if err != nil {
				continue
			}

			entry, err := table.Get(key)
			if err == sstable.ErrNotFound {
				common.Logf("get key=%q: not in L%d/%d.sst\n", string(key), level, fileNo)
				continue
			}
			if err != nil {
				return nil, err
			}

			if entry.Type == common.EntryTypeDelete {
				common.Logf("get key=%q: found tombstone in L%d/%d.sst\n", string(key), level, fileNo)
				return nil, ErrNotFound
			}
			common.Logf("get key=%q: found in L%d/%d.sst\n", string(key), level, fileNo)
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
	newWALPath := common.WALPath(newWALNum)
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
	path := common.SSTablePath(0, fileNo)
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	// Get sorted entries from memtable
	iter := d.memtable.Iterator()

	// Write all entries to SSTable
	_, err = sstable.WriteSSTable(f, iter)
	if err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// Update manifest to add new SSTable to L0
	edit := &manifest.CompactionEdit{
		AddSSTables: map[int]map[common.FileNo]struct{}{
			0: {fileNo: {}},
		},
	}
	d.manifest.Apply(edit)

	common.LogDuration(start, "flushed wal to %d.sst", fileNo)
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

