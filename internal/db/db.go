package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"amethyst/internal/common"
	"amethyst/internal/manifest"
	"amethyst/internal/memtable"
	"amethyst/internal/wal"
)

type options struct {
	walThreshold    int
	maxSSTableLevel int
}

type Option func(*options)

func WithWALThreshold(n int) Option {
	return func(o *options) {
		o.walThreshold = n
	}
}

func WithMaxSSTableLevel(n int) Option {
	return func(o *options) {
		o.maxSSTableLevel = n
	}
}

type DB struct {
	mu       sync.RWMutex
	nextSeq  uint64
	memtable memtable.Memtable
	wal      wal.WAL
	manifest *manifest.Manifest
	opts     options
}

func Open(optFns ...Option) (*DB, error) {
	opts := options{
		walThreshold:    1000,
		maxSSTableLevel: 3,
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	// Create directories
	if err := os.MkdirAll("wal", 0755); err != nil {
		return nil, err
	}
	for i := 0; i <= opts.maxSSTableLevel; i++ {
		if err := os.MkdirAll(fmt.Sprintf("sstable/%d", i), 0755); err != nil {
			return nil, err
		}
	}

	m := manifest.NewManifest(opts.maxSSTableLevel + 1)

	// Create initial WAL
	walPath := fmt.Sprintf("wal/%d.log", m.Current().NextWALNumber)
	log, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	m.SetWAL(m.Current().NextWALNumber)

	return &DB{
		memtable: memtable.NewMapMemtable(),
		wal:      log,
		manifest: m,
		opts:     opts,
	}, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if we need to rotate WAL
	if d.wal.Len() >= d.opts.walThreshold {
		if err := d.rotateWAL(); err != nil {
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
	if err := d.wal.Append([]*common.Entry{entry}); err != nil {
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

	// Check if we need to rotate WAL
	if d.wal.Len() >= d.opts.walThreshold {
		if err := d.rotateWAL(); err != nil {
			return err
		}
	}

	d.nextSeq++

	entry := &common.Entry{
		Type: common.EntryTypeDelete,
		Seq:  d.nextSeq,
		Key:  bytes.Clone(key),
	}
	if err := d.wal.Append([]*common.Entry{entry}); err != nil {
		return err
	}

	d.memtable.Delete(entry.Key)
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	value, ok := d.memtable.Get(key)
	if !ok {
		return nil, errors.New("db: not found")
	}
	return bytes.Clone(value), nil
}

// rotateWAL creates a new WAL and flushes the current memtable.
// Must be called with d.mu held.
func (d *DB) rotateWAL() error {
	v := d.manifest.Current()
	newWALNum := v.NextWALNumber

	// 1. Create new WAL file
	newWALPath := fmt.Sprintf("wal/%d.log", newWALNum)
	newWAL, err := wal.NewWAL(newWALPath)
	if err != nil {
		return err
	}

	// 2. Flush current memtable to SSTable (stubbed for now)
	if err := d.flushMemtable(); err != nil {
		return err
	}

	// 3. Update manifest (atomic commit point)
	d.manifest.SetWAL(newWALNum)

	// 4. Swap to new WAL and new memtable
	oldWAL := d.wal
	oldWALNum := v.CurrentWAL
	d.wal = newWAL
	d.memtable = memtable.NewMapMemtable()

	// 5. Close and delete old WAL
	if closer, ok := oldWAL.(interface{ Close() error }); ok {
		closer.Close()
	}
	oldWALPath := fmt.Sprintf("wal/%d.log", oldWALNum)
	os.Remove(oldWALPath)

	return nil
}

// flushMemtable writes the current memtable to an SSTable.
// Stubbed for now - will be implemented when SSTable writer is ready.
func (d *DB) flushMemtable() error {
	// TODO: Implement SSTable flush
	// 1. Get iterator from memtable
	// 2. Write all entries to SSTable
	// 3. Update manifest with new SSTable
	return nil
}
