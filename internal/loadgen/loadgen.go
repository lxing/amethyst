// Package loadgen drives a configurable background workload against a store.
//
// It is meant to keep the engine busy while you watch it (e.g. from a live
// visualization): a fixed pool of workers issues a weighted mix of inserts,
// updates, deletes and gets at a target rate that can be changed while running.
//
// Keys come from two independent key spaces:
//   - sequential: s0000000000, s0000000001, ... (ascending inserts, so each
//     flushed L0 file covers a disjoint key range)
//   - scattered:  r<permuted index> (inserts land all over the key range, so
//     every L0 file overlaps every other — the classic LSM picture)
//
// Each space has its own insert counter, so switching orders mid-run keeps
// updates/gets/deletes aimed at keys that actually exist.
package loadgen

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Store is the subset of the DB API the generator drives.
type Store interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
}

// IsNotFound lets the generator tell a legitimate miss (deleted or not yet
// inserted key) from a failure, e.g. func(err error) bool { return errors.Is(err, db.ErrNotFound) }.
type IsNotFound func(error) bool

// Op is a workload operation type.
type Op int

const (
	OpInsert Op = iota // write a brand-new key
	OpUpdate           // overwrite an existing key
	OpDelete           // tombstone an existing key
	OpGet              // read an existing key
	numOps
)

var opNames = [numOps]string{"insert", "update", "delete", "get"}

func (o Op) String() string { return opNames[o] }

// ParseOp maps a name like "insert" to its Op.
func ParseOp(name string) (Op, error) {
	for i, n := range opNames {
		if n == name {
			return Op(i), nil
		}
	}
	return 0, fmt.Errorf("unknown op %q (want insert|update|delete|get)", name)
}

// Dist selects which existing key update/delete/get operations target.
type Dist string

const (
	DistUniform Dist = "uniform" // every existing key equally likely
	DistZipf    Dist = "zipf"    // skewed toward recently inserted keys
)

// Order selects how inserted keys are laid out in the key range.
type Order string

const (
	OrderSequential Order = "sequential"
	OrderScattered  Order = "scattered"
)

// Config is the live-adjustable workload description.
type Config struct {
	Rate      int         `json:"rate"`       // target ops/sec across all workers; 0 = as fast as possible
	Mix       [numOps]int `json:"mix"`        // relative weights per Op: insert, update, delete, get
	Dist      Dist        `json:"dist"`       // key choice for update/delete/get
	Order     Order       `json:"order"`      // key layout for inserts (and the space other ops draw from)
	ValueSize int         `json:"value_size"` // bytes per value (minimum is the self-describing prefix)
}

// DefaultConfig flushes a 256-entry memtable a couple of times a second and
// gives compaction overwrites and tombstones to resolve.
var DefaultConfig = Config{
	Rate:      500,
	Mix:       [numOps]int{OpInsert: 50, OpUpdate: 30, OpDelete: 5, OpGet: 15},
	Dist:      DistZipf,
	Order:     OrderScattered,
	ValueSize: 64,
}

// Validate reports whether the config can drive a workload.
func (c Config) Validate() error {
	if c.Rate < 0 {
		return errors.New("rate must be >= 0")
	}
	total := 0
	for _, w := range c.Mix {
		if w < 0 {
			return errors.New("mix weights must be >= 0")
		}
		total += w
	}
	if total == 0 {
		return errors.New("mix must have at least one positive weight")
	}
	if c.Dist != DistUniform && c.Dist != DistZipf {
		return fmt.Errorf("unknown dist %q (want uniform|zipf)", c.Dist)
	}
	if c.Order != OrderSequential && c.Order != OrderScattered {
		return fmt.Errorf("unknown order %q (want sequential|scattered)", c.Order)
	}
	if c.ValueSize < 0 {
		return errors.New("value size must be >= 0")
	}
	return nil
}

// MixString renders the mix as "insert:50,update:30,...".
func (c Config) MixString() string {
	parts := make([]string, 0, numOps)
	for op, w := range c.Mix {
		if w > 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", Op(op), w))
		}
	}
	return strings.Join(parts, ",")
}

// ParseMix parses "insert:50,update:30" into weights. Unnamed ops get weight 0.
func ParseMix(s string) ([numOps]int, error) {
	var mix [numOps]int
	for part := range strings.SplitSeq(s, ",") {
		name, weight, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return mix, fmt.Errorf("bad mix entry %q (want op:weight)", part)
		}
		op, err := ParseOp(name)
		if err != nil {
			return mix, err
		}
		w, err := strconv.Atoi(weight)
		if err != nil || w < 0 {
			return mix, fmt.Errorf("bad weight %q for %s", weight, name)
		}
		mix[op] = w
	}
	return mix, nil
}

// KeySpace records how many keys have been inserted into each key space.
// Persist it alongside the DB so a reloaded checkpoint keeps hitting real keys.
type KeySpace struct {
	Sequential uint64 `json:"sequential"`
	Scattered  uint64 `json:"scattered"`
}

// OpStats summarizes one operation type.
type OpStats struct {
	Count        uint64        `json:"count"`
	Errors       uint64        `json:"errors"`
	TotalLatency time.Duration `json:"total_latency_ns"`
	MaxLatency   time.Duration `json:"max_latency_ns"`
}

// AvgLatency is the mean latency of successful and failed calls.
func (s OpStats) AvgLatency() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return s.TotalLatency / time.Duration(s.Count)
}

// Stats is a point-in-time copy of the generator's counters for the current run.
type Stats struct {
	Running   bool               `json:"running"`
	Elapsed   time.Duration      `json:"elapsed_ns"`
	Workers   int                `json:"workers"`
	Config    Config             `json:"config"`
	Ops       map[string]OpStats `json:"ops"`
	NotFound  uint64             `json:"not_found"` // gets that legitimately missed (deleted or not yet inserted)
	Corrupt   uint64             `json:"corrupt"`   // gets that returned another key's value
	Keys      KeySpace           `json:"keys"`
	LastError string             `json:"last_error,omitempty"`
}

// TotalOps sums Count across all operation types.
func (s Stats) TotalOps() uint64 {
	var n uint64
	for _, o := range s.Ops {
		n += o.Count
	}
	return n
}

type opCounters struct {
	count, errors, totalNanos, maxNanos atomic.Uint64
}

func (c *opCounters) record(d time.Duration, err error) {
	c.count.Add(1)
	if err != nil {
		c.errors.Add(1)
	}
	n := uint64(d)
	c.totalNanos.Add(n)
	for {
		cur := c.maxNanos.Load()
		if n <= cur || c.maxNanos.CompareAndSwap(cur, n) {
			return
		}
	}
}

const (
	pacingInterval = 5 * time.Millisecond
	tokenBuffer    = 4096 // caps unthrottled mode at ~800k ops/s
	zipfSkew       = 1.1
)

// Generator runs workloads against a Store. It can be started and stopped
// repeatedly; the key space carries over between runs.
type Generator struct {
	store      Store
	isNotFound IsNotFound

	sequential atomic.Uint64
	scattered  atomic.Uint64

	cfg atomic.Pointer[Config]

	// Per-run state, guarded by mu for Start/Stop transitions.
	mu        sync.Mutex
	running   bool
	workers   int
	startedAt time.Time
	stoppedAt time.Time
	stopCh    chan struct{}
	tokens    chan struct{}
	wg        sync.WaitGroup

	ops       [numOps]opCounters
	notFound  atomic.Uint64
	corrupt   atomic.Uint64
	lastError atomic.Pointer[string]
	valueSeq  atomic.Uint64
}

// New creates a generator that resumes from the given key space.
func New(store Store, isNotFound IsNotFound, keys KeySpace) *Generator {
	g := &Generator{store: store, isNotFound: isNotFound}
	g.sequential.Store(keys.Sequential)
	g.scattered.Store(keys.Scattered)
	cfg := DefaultConfig
	g.cfg.Store(&cfg)
	return g
}

// Start launches a new run with the given config. Counters reset for each run.
func (g *Generator) Start(cfg Config, workers int) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if workers < 1 {
		return errors.New("workers must be >= 1")
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running {
		return errors.New("load already running")
	}

	g.cfg.Store(&cfg)
	g.resetCounters()
	g.running = true
	g.workers = workers
	g.startedAt = time.Now()
	g.stopCh = make(chan struct{})
	g.tokens = make(chan struct{}, tokenBuffer)

	g.wg.Add(1 + workers)
	go g.pace()
	for i := range workers {
		go g.work(uint64(i), workers)
	}
	return nil
}

// Stop signals all workers and waits until every in-flight operation returns.
// Once Stop returns, the generator has no outstanding calls into the store,
// so the caller may do stop-the-world work (e.g. checkpointing) safely.
// Counters are kept until the next Start.
func (g *Generator) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.running {
		return
	}
	close(g.stopCh)
	g.wg.Wait()
	g.running = false
	g.stoppedAt = time.Now()
}

// Running reports whether workers are active.
func (g *Generator) Running() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.running
}

// SetConfig swaps the workload config; running workers pick it up on their next op.
func (g *Generator) SetConfig(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	g.cfg.Store(&cfg)
	return nil
}

// Config returns the current workload config.
func (g *Generator) Config() Config {
	return *g.cfg.Load()
}

// KeySpace returns how many keys have been inserted into each space.
func (g *Generator) KeySpace() KeySpace {
	return KeySpace{Sequential: g.sequential.Load(), Scattered: g.scattered.Load()}
}

// Stats returns a copy of the current run's counters.
func (g *Generator) Stats() Stats {
	g.mu.Lock()
	running, workers, startedAt, stoppedAt := g.running, g.workers, g.startedAt, g.stoppedAt
	g.mu.Unlock()

	s := Stats{
		Running:  running,
		Workers:  workers,
		Config:   g.Config(),
		Ops:      make(map[string]OpStats, numOps),
		NotFound: g.notFound.Load(),
		Corrupt:  g.corrupt.Load(),
		Keys:     g.KeySpace(),
	}
	// Elapsed freezes once the run stops, so an idle generator's stats are stable.
	switch {
	case running:
		s.Elapsed = time.Since(startedAt)
	case !startedAt.IsZero():
		s.Elapsed = stoppedAt.Sub(startedAt)
	}
	for op := range g.ops {
		c := &g.ops[op]
		s.Ops[Op(op).String()] = OpStats{
			Count:        c.count.Load(),
			Errors:       c.errors.Load(),
			TotalLatency: time.Duration(c.totalNanos.Load()),
			MaxLatency:   time.Duration(c.maxNanos.Load()),
		}
	}
	if e := g.lastError.Load(); e != nil {
		s.LastError = *e
	}
	return s
}

func (g *Generator) resetCounters() {
	for op := range g.ops {
		c := &g.ops[op]
		c.count.Store(0)
		c.errors.Store(0)
		c.totalNanos.Store(0)
		c.maxNanos.Store(0)
	}
	g.notFound.Store(0)
	g.corrupt.Store(0)
	g.lastError.Store(nil)
}

// pace releases rate tokens every pacingInterval. Rate 0 keeps the bucket full.
// Credit is never banked beyond what workers can absorb, so a slow stretch
// (e.g. a memtable flush) doesn't turn into a burst afterwards.
func (g *Generator) pace() {
	defer g.wg.Done()
	ticker := time.NewTicker(pacingInterval)
	defer ticker.Stop()

	last := time.Now()
	credit := 0.0
	for {
		select {
		case <-g.stopCh:
			return
		case now := <-ticker.C:
			rate := g.cfg.Load().Rate
			if rate == 0 {
				credit = tokenBuffer
			} else {
				credit += float64(rate) * now.Sub(last).Seconds()
			}
			last = now

		fill:
			for credit >= 1 {
				select {
				case g.tokens <- struct{}{}:
					credit--
				default:
					credit = 0
					break fill
				}
			}
		}
	}
}

func (g *Generator) work(id uint64, workers int) {
	defer g.wg.Done()
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), id))
	zipf := rand.NewZipf(rng, zipfSkew, 1, 1<<32)

	for {
		select {
		case <-g.stopCh:
			return
		case <-g.tokens:
		}
		g.doOp(rng, zipf, g.cfg.Load(), uint64(workers))
	}
}

func (g *Generator) doOp(rng *rand.Rand, zipf *rand.Zipf, cfg *Config, workers uint64) {
	op := pickOp(rng, cfg.Mix)
	counter := &g.scattered
	if cfg.Order == OrderSequential {
		counter = &g.sequential
	}

	// An insert claims its index before its Put commits (~BatchTimeout later),
	// so the newest indices may not exist yet. At most one insert per worker is
	// in flight, so skip the newest `workers` indices once the space is big enough;
	// otherwise zipf-skewed gets mostly hit keys that are still being written.
	n := counter.Load()
	if n > workers {
		n -= workers
	}

	// Nothing to update/delete/read yet: seed the space instead.
	if n == 0 {
		op = OpInsert
	}

	var idx uint64
	if op == OpInsert {
		idx = counter.Add(1) - 1
	} else {
		idx = pickExisting(rng, zipf, n, cfg.Dist)
	}
	key := formatKey(cfg.Order, idx)

	start := time.Now()
	var err error
	switch op {
	case OpInsert, OpUpdate:
		err = g.store.Put(key, g.makeValue(key, cfg.ValueSize))
	case OpDelete:
		err = g.store.Delete(key)
	case OpGet:
		var val []byte
		val, err = g.store.Get(key)
		switch {
		case err != nil && g.isNotFound != nil && g.isNotFound(err):
			g.notFound.Add(1)
			err = nil
		case err == nil && !validValue(key, val):
			g.corrupt.Add(1)
		}
	}
	g.ops[op].record(time.Since(start), err)
	if err != nil {
		msg := fmt.Sprintf("%s %s: %v", op, key, err)
		g.lastError.Store(&msg)
	}
}

func pickOp(rng *rand.Rand, mix [numOps]int) Op {
	total := 0
	for _, w := range mix {
		total += w
	}
	r := rng.IntN(total)
	for op, w := range mix {
		if r < w {
			return Op(op)
		}
		r -= w
	}
	return OpInsert // unreachable with a validated mix
}

// pickExisting chooses an index in [0, n). Zipf favors the most recent inserts.
func pickExisting(rng *rand.Rand, zipf *rand.Zipf, n uint64, dist Dist) uint64 {
	if dist == DistZipf {
		return n - 1 - zipf.Uint64()%n
	}
	return rng.Uint64N(n)
}

// formatKey renders a fixed-width key so byte order matches numeric order.
// Scattered keys multiply by an odd constant mod 2^32, a bijection, so every
// index still maps to a distinct key.
func formatKey(order Order, idx uint64) []byte {
	if order == OrderScattered {
		return fmt.Appendf(nil, "r%010d", uint32(idx)*2654435761)
	}
	return fmt.Appendf(nil, "s%010d", idx)
}

// makeValue embeds the key so a get can detect cross-key corruption.
func (g *Generator) makeValue(key []byte, size int) []byte {
	v := fmt.Appendf(nil, "v:%s:%d:", key, g.valueSeq.Add(1))
	for len(v) < size {
		v = append(v, '.')
	}
	return v
}

func validValue(key, val []byte) bool {
	prefix := "v:" + string(key) + ":"
	return strings.HasPrefix(string(val), prefix)
}
