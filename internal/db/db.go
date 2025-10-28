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
	WALThreshold    int
	MaxSSTableLevel int
}

var DefaultOptions = Options{
	WALThreshold:    256,
	MaxSSTableLevel: 3,
}

type Option func(*Options)

func WithWALThreshold(n int) Option {
	return func(o *Options) {
		o.WALThreshold = n
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

	m := manifest.NewManifest(opts.MaxSSTableLevel + 1)

	// Create initial WAL
	walPath := common.WALPath(m.Current().NextWALNumber)
	log, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	m.SetWAL(m.Current().NextWALNumber)

	return &DB{
		memtable: memtable.NewMapMemtable(),
		wal:      log,
		manifest: m,
		Opts:     opts,
	}, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if we need to flush memtable
	if d.wal.Len() >= d.Opts.WALThreshold {
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
	if d.wal.Len() >= d.Opts.WALThreshold {
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

	// Check memtable first
	entry, ok := d.memtable.Get(key)
	if ok {
		// Found in memtable
		if entry.Type == common.EntryTypeDelete {
			return nil, ErrNotFound
		}
		return bytes.Clone(entry.Value), nil
	}

	// Not in memtable, search SSTables from newest to oldest
	version := d.manifest.Current()
	for level, fileNos := range version.Levels {
		// TODO: Optimize lookup for L1+
		// L0 files have overlapping ranges, so we must check all files.
		// L1+ files are non-overlapping within a level, so we can binary search
		// by key range to find the single file that might contain the key.
		for _, fileNo := range fileNos {
			table, err := d.manifest.GetTable(fileNo, level)
			if err != nil {
				// Table might not exist yet, continue
				continue
			}

			entry, err := table.Get(key)
			if err == sstable.ErrNotFound {
				// Not in this table, continue
				continue
			}
			if err != nil {
				// Real error
				return nil, err
			}

			// Found it
			if entry.Type == common.EntryTypeDelete {
				return nil, ErrNotFound
			}
			return bytes.Clone(entry.Value), nil
		}
	}

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
	newWAL, err := wal.NewWAL(newWALPath)
	if err != nil {
		return err
	}

	// 3. Write memtable to SSTable
	if err := d.writeSSTable(); err != nil {
		return err
	}

	// 4. Update manifest (atomic commit point)
	d.manifest.SetWAL(newWALNum)

	// 5. Swap to new WAL and new memtable
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

