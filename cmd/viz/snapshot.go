package main

import (
	"os"
	"time"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/loadgen"
	"amethyst/internal/manifest"
)

// Snapshot is the metadata view streamed to the browser. It is built from the
// engine's in-memory state only; data files are never re-opened by path.
type Snapshot struct {
	// Epoch changes whenever the DB is replaced (restore/clear) so the client
	// can drop its history instead of animating the jump.
	Epoch    int           `json:"epoch"`
	Options  OptionsView   `json:"options"`
	Memtable MemtableView  `json:"memtable"`
	WAL      WALView       `json:"wal"`
	Levels   [][]FileView  `json:"levels"`
	Load     loadgen.Stats `json:"load"`
}

type OptionsView struct {
	FlushThreshold int     `json:"flush_threshold"`
	MaxBatchSize   int     `json:"max_batch_size"`
	BatchTimeoutMs float64 `json:"batch_timeout_ms"`
}

type MemtableView struct {
	Count      int      `json:"count"`
	Puts       []string `json:"puts"`       // sorted sample of live keys
	Tombstones []string `json:"tombstones"` // sorted sample of deleted keys
}

type WALView struct {
	FileNo common.FileNo `json:"file_no"`
	Bytes  int64         `json:"bytes"`
}

type FileView struct {
	FileNo   common.FileNo `json:"file_no"`
	Smallest string        `json:"smallest"`
	Largest  string        `json:"largest"`
	Entries  int           `json:"entries"`
	Bytes    int64         `json:"bytes"`
	Blocks   []string      `json:"blocks"` // first key of each data block
}

// maxMemtableKeys caps the memtable sample so frames stay small with large thresholds.
const maxMemtableKeys = 512

// tableCache holds per-file metadata. SSTables are immutable, so each file is
// read once (through the manifest's own table cache) and reused every frame.
// Owned by the sampler goroutine only.
type tableCache map[common.FileNo]FileView

func (tc tableCache) get(engine *db.DB, level int, fm manifest.FileMetadata) (FileView, bool) {
	if fv, ok := tc[fm.FileNo]; ok {
		return fv, true
	}
	table, err := engine.Manifest().GetTable(fm.FileNo, level)
	if err != nil {
		return FileView{}, false
	}
	index := table.GetIndex()
	blocks := make([]string, len(index.Entries))
	for i, e := range index.Entries {
		blocks[i] = string(e.Key)
	}
	var size int64
	if fi, err := os.Stat(engine.Paths().SSTablePath(level, fm.FileNo)); err == nil {
		size = fi.Size()
	}
	fv := FileView{
		FileNo:   fm.FileNo,
		Smallest: string(fm.SmallestKey),
		Largest:  string(fm.LargestKey),
		Entries:  table.Len(),
		Bytes:    size,
		Blocks:   blocks,
	}
	tc[fm.FileNo] = fv
	return fv, true
}

// buildSnapshot reads the memtable BEFORE the version. Flush publishes the new
// version (with the new L0 file) before swapping in an empty memtable, so this
// order can briefly show a flushed batch twice but never shows data missing.
func buildSnapshot(engine *db.DB, gen *loadgen.Generator, epoch int, tables tableCache) Snapshot {
	mem := memtableView(engine)
	v := engine.Manifest().Current()

	snap := Snapshot{
		Epoch: epoch,
		Options: OptionsView{
			FlushThreshold: engine.Opts.MemtableFlushThreshold,
			MaxBatchSize:   engine.Opts.MaxBatchSize,
			BatchTimeoutMs: float64(engine.Opts.BatchTimeout) / float64(time.Millisecond),
		},
		Memtable: mem,
		WAL:      WALView{FileNo: v.CurrentWAL},
		Levels:   make([][]FileView, len(v.Levels)),
		Load:     gen.Stats(),
	}
	if fi, err := os.Stat(engine.Paths().WALPath(v.CurrentWAL)); err == nil {
		snap.WAL.Bytes = fi.Size()
	}

	live := make(map[common.FileNo]bool)
	for level, files := range v.Levels {
		snap.Levels[level] = make([]FileView, 0, len(files))
		for _, f := range files {
			fv, ok := tables.get(engine, level, f)
			if !ok {
				continue
			}
			live[f.FileNo] = true
			snap.Levels[level] = append(snap.Levels[level], fv)
		}
	}
	for fileNo := range tables {
		if !live[fileNo] {
			delete(tables, fileNo)
		}
	}
	return snap
}

func memtableView(engine *db.DB) MemtableView {
	mt := engine.Memtable()
	iter := mt.Iterator() // sorted copy taken under the memtable's own lock

	var puts, tombs []string
	for {
		e, err := iter.Next()
		if err != nil || e == nil {
			break
		}
		if e.Type == common.EntryTypeDelete {
			tombs = append(tombs, string(e.Key))
		} else {
			puts = append(puts, string(e.Key))
		}
	}
	count := len(puts) + len(tombs)
	return MemtableView{
		Count:      count,
		Puts:       sample(puts, maxMemtableKeys*len(puts)/max(count, 1)),
		Tombstones: sample(tombs, maxMemtableKeys*len(tombs)/max(count, 1)),
	}
}

// sample keeps at most n evenly spaced elements, preserving order.
func sample(keys []string, n int) []string {
	if keys == nil {
		return []string{}
	}
	if len(keys) <= n {
		return keys
	}
	out := make([]string, n)
	for i := range n {
		out[i] = keys[i*len(keys)/n]
	}
	return out
}
