package loadgen_test

import (
	"errors"
	"testing"
	"time"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/loadgen"

	"github.com/stretchr/testify/require"
)

// TestLoadAgainstDB runs a mixed workload across several memtable flushes and
// checks that no read ever returns another key's value.
func TestLoadAgainstDB(t *testing.T) {
	common.LoggingEnabled = false
	dir := t.TempDir()
	d, err := db.Open(db.WithDBPath(dir), db.WithMemtableFlushThreshold(64))
	require.NoError(t, err)

	isNotFound := func(err error) bool { return errors.Is(err, db.ErrNotFound) }
	g := loadgen.New(d, isNotFound, loadgen.KeySpace{})

	for _, order := range []loadgen.Order{loadgen.OrderScattered, loadgen.OrderSequential} {
		cfg := loadgen.DefaultConfig
		cfg.Rate = 0
		cfg.Order = order
		require.NoError(t, g.Start(cfg, 32))
		time.Sleep(300 * time.Millisecond)
		g.Stop()

		s := g.Stats()
		require.Positive(t, s.Ops["insert"].Count, order)
		require.Positive(t, s.Ops["get"].Count, order)
		require.Zero(t, s.Corrupt, order)
		for name, o := range s.Ops {
			require.Zero(t, o.Errors, "%s %s: %s", order, name, s.LastError)
		}
	}

	ks := g.KeySpace()
	require.Positive(t, ks.Sequential)
	require.Positive(t, ks.Scattered)
	require.NotEmpty(t, d.Manifest().Current().Levels[0], "load should have flushed L0 files")

	// After the load stops, a read-only pass over a slice of every key space
	// sees only valid values or deletions.
	verify := loadgen.DefaultConfig
	verify.Rate = 0
	verify.Mix = [4]int{loadgen.OpGet: 1}
	verify.Dist = loadgen.DistUniform
	require.NoError(t, g.Start(verify, 8))
	time.Sleep(100 * time.Millisecond)
	g.Stop()
	require.Zero(t, g.Stats().Corrupt)
}
