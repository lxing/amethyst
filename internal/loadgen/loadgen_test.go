package loadgen

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errMissing = errors.New("missing")

// memStore is a concurrency-safe in-memory Store that tracks in-flight calls.
type memStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	inflight atomic.Int64
	calls    atomic.Int64
}

func newMemStore() *memStore { return &memStore{data: map[string][]byte{}} }

func (m *memStore) enter() func() {
	m.calls.Add(1)
	m.inflight.Add(1)
	return func() { m.inflight.Add(-1) }
}

func (m *memStore) Put(k, v []byte) error {
	defer m.enter()()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(k)] = append([]byte(nil), v...)
	return nil
}

func (m *memStore) Delete(k []byte) error {
	defer m.enter()()
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(k))
	return nil
}

func (m *memStore) Get(k []byte) ([]byte, error) {
	defer m.enter()()
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[string(k)]
	if !ok {
		return nil, errMissing
	}
	return v, nil
}

func isMissing(err error) bool { return errors.Is(err, errMissing) }

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "default", mutate: func(*Config) {}},
		{name: "unthrottled", mutate: func(c *Config) { c.Rate = 0 }},
		{name: "negative rate", mutate: func(c *Config) { c.Rate = -1 }, wantErr: true},
		{name: "empty mix", mutate: func(c *Config) { c.Mix = [numOps]int{} }, wantErr: true},
		{name: "negative weight", mutate: func(c *Config) { c.Mix[OpGet] = -1 }, wantErr: true},
		{name: "bad dist", mutate: func(c *Config) { c.Dist = "normal" }, wantErr: true},
		{name: "bad order", mutate: func(c *Config) { c.Order = "reverse" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig
			tt.mutate(&cfg)
			require.Equal(t, tt.wantErr, cfg.Validate() != nil)
		})
	}
}

func TestParseMix(t *testing.T) {
	t.Parallel()
	mix, err := ParseMix("insert:10, get:5")
	require.NoError(t, err)
	require.Equal(t, [numOps]int{OpInsert: 10, OpGet: 5}, mix)

	// Round-trips through MixString.
	mix, err = ParseMix(DefaultConfig.MixString())
	require.NoError(t, err)
	require.Equal(t, DefaultConfig.Mix, mix)

	for _, bad := range []string{"insert", "scan:5", "get:x", "get:-1"} {
		_, err := ParseMix(bad)
		require.Error(t, err, bad)
	}
}

func TestFormatKey(t *testing.T) {
	t.Parallel()

	// Sequential keys sort in insertion order.
	var seq []string
	for i := range uint64(1000) {
		seq = append(seq, string(formatKey(OrderSequential, i)))
	}
	require.True(t, sort.StringsAreSorted(seq))

	// Scattered keys are distinct, fixed width, and not in insertion order.
	seen := map[string]bool{}
	var scattered []string
	for i := range uint64(1000) {
		k := string(formatKey(OrderScattered, i))
		require.Len(t, k, 11)
		require.False(t, seen[k], "duplicate key %s", k)
		seen[k] = true
		scattered = append(scattered, k)
	}
	require.False(t, sort.StringsAreSorted(scattered))
}

func TestRatePacing(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	g := New(store, isMissing, KeySpace{})

	cfg := DefaultConfig
	cfg.Rate = 400
	require.NoError(t, g.Start(cfg, 8))
	time.Sleep(500 * time.Millisecond)
	g.Stop()

	// ~200 ops expected; allow generous slack for scheduler jitter.
	ops := g.Stats().TotalOps()
	require.InDelta(t, 200, float64(ops), 100, "ops=%d", ops)
}

func TestStopDrainsInflight(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	g := New(store, isMissing, KeySpace{})

	cfg := DefaultConfig
	cfg.Rate = 0
	require.NoError(t, g.Start(cfg, 16))
	time.Sleep(50 * time.Millisecond)
	g.Stop()

	require.False(t, g.Running())
	require.Zero(t, store.inflight.Load())

	// No calls reach the store after Stop returns.
	calls := store.calls.Load()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, calls, store.calls.Load())

	// Stopping twice is a no-op; starting again works and resets counters.
	g.Stop()
	require.NoError(t, g.Start(cfg, 2))
	defer g.Stop()
	require.Error(t, g.Start(cfg, 2), "double start")
}

func TestSetConfigLive(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	g := New(store, isMissing, KeySpace{})

	cfg := DefaultConfig
	cfg.Rate = 0
	require.NoError(t, g.Start(cfg, 4))
	defer g.Stop()
	time.Sleep(30 * time.Millisecond)

	// Switch to read-only: the key space must stop growing.
	readOnly := cfg
	readOnly.Mix = [numOps]int{OpGet: 1}
	require.NoError(t, g.SetConfig(readOnly))
	time.Sleep(10 * time.Millisecond) // let in-flight inserts land
	before := g.KeySpace()
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, before, g.KeySpace())
	require.Zero(t, g.Stats().Corrupt)

	require.Error(t, g.SetConfig(Config{}), "invalid config rejected")
}

func TestKeySpacesAreIndependent(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	g := New(store, isMissing, KeySpace{})

	cfg := DefaultConfig
	cfg.Rate = 0
	cfg.Mix = [numOps]int{OpInsert: 1}
	cfg.Order = OrderSequential
	require.NoError(t, g.Start(cfg, 4))
	time.Sleep(20 * time.Millisecond)
	g.Stop()

	ks := g.KeySpace()
	require.Positive(t, ks.Sequential)
	require.Zero(t, ks.Scattered)

	// Every sequential index below the counter was written.
	for i := range ks.Sequential {
		_, err := store.Get(formatKey(OrderSequential, i))
		require.NoError(t, err)
	}
}

func TestKeySpaceStateRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ks, err := LoadKeySpace(dir)
	require.NoError(t, err)
	require.Equal(t, KeySpace{}, ks, "missing file means fresh")

	want := KeySpace{Sequential: 12, Scattered: 345}
	require.NoError(t, SaveKeySpace(dir, want))
	ks, err = LoadKeySpace(dir)
	require.NoError(t, err)
	require.Equal(t, want, ks)

	// A generator resumed from it keeps inserting after the saved counters.
	g := New(newMemStore(), isMissing, ks)
	require.Equal(t, want, g.KeySpace())
}
