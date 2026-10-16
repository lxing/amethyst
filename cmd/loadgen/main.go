// Command loadgen drives a background workload against an Amethyst DB and
// manages on-disk checkpoints of the DB directory.
//
//	loadgen run [flags]        run load until -duration elapses or Ctrl-C
//	loadgen save <name>        checkpoint the DB directory
//	loadgen restore <name>     replace the DB directory with a checkpoint
//	loadgen ls                 list checkpoints
//	loadgen rm <name>          delete a checkpoint
//
// Only one process may have a DB open at a time; there is no lock file yet,
// so don't point loadgen at a DB the adb REPL has open.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"amethyst/internal/checkpoint"
	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/loadgen"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  loadgen run [flags]                 run load until -duration elapses or Ctrl-C
  loadgen save [-db dir] <name>       checkpoint the DB directory
  loadgen restore [-db dir] <name>    replace the DB directory with a checkpoint
  loadgen ls [-checkpoints dir]       list checkpoints
  loadgen rm [-checkpoints dir] <name>

run 'loadgen <command> -h' for flags`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "run":
		err = runCmd(args)
	case "save":
		err = saveCmd(args)
	case "restore":
		err = restoreCmd(args)
	case "ls":
		err = lsCmd(args)
	case "rm":
		err = rmCmd(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

// dirFlags registers the DB and checkpoint directory flags shared by all commands.
func dirFlags(fs *flag.FlagSet) (dbDir, ckptDir *string) {
	dbDir = fs.String("db", "data/adb", "DB directory")
	ckptDir = fs.String("checkpoints", "", "checkpoint directory (default: <db parent>/checkpoints)")
	return dbDir, ckptDir
}

func checkpointRoot(dbDir, ckptDir string) string {
	if ckptDir != "" {
		return ckptDir
	}
	return filepath.Join(filepath.Dir(filepath.Clean(dbDir)), "checkpoints")
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dbDir, ckptDir := dirFlags(fs)
	def := loadgen.DefaultConfig
	rate := fs.Int("rate", def.Rate, "target ops/sec across all workers (0 = unthrottled)")
	workers := fs.Int("workers", 32, "concurrent workers (writes block ~BatchTimeout on group commit, so rate needs workers)")
	mix := fs.String("mix", def.MixString(), "op weights, e.g. insert:50,update:30,delete:5,get:15")
	dist := fs.String("dist", string(def.Dist), "key choice for update/delete/get: uniform|zipf")
	order := fs.String("order", string(def.Order), "insert key layout: sequential|scattered")
	valueSize := fs.Int("value", def.ValueSize, "value size in bytes")
	duration := fs.Duration("duration", 0, "how long to run (0 = until Ctrl-C)")
	interval := fs.Duration("stats", time.Second, "stats print interval")
	from := fs.String("from", "", "restore this checkpoint before running")
	save := fs.String("save", "", "save a checkpoint with this name after stopping")
	// Fatter than the engine defaults so sustained load doesn't hammer the
	// filesystem: fewer WAL rotations/SSTables and fewer fsyncs per second.
	flushThreshold := fs.Int("flush-threshold", 1024, "memtable entries before flush (and WAL rotation)")
	batchSize := fs.Int("batch-size", 256, "max writes per group commit (fsync); capped in practice by -workers")
	batchTimeout := fs.Duration("batch-timeout", 20*time.Millisecond, "max wait to fill a group commit; adds up to this much write latency")
	fs.Parse(args)

	cfg := loadgen.Config{
		Rate:      *rate,
		Dist:      loadgen.Dist(*dist),
		Order:     loadgen.Order(*order),
		ValueSize: *valueSize,
	}
	var err error
	if cfg.Mix, err = loadgen.ParseMix(*mix); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	root := checkpointRoot(*dbDir, *ckptDir)
	if *from != "" {
		if err := checkpoint.Restore(root, *from, *dbDir); err != nil {
			return err
		}
		fmt.Printf("restored checkpoint %q into %s\n", *from, *dbDir)
	}

	// Per-op engine logging would drown the stats line.
	common.LoggingEnabled = false
	engine, err := db.Open(
		db.WithDBPath(*dbDir),
		db.WithMemtableFlushThreshold(*flushThreshold),
		db.WithMaxBatchSize(*batchSize),
		db.WithBatchTimeout(*batchTimeout),
	)
	if err != nil {
		return err
	}

	keys, err := loadgen.LoadKeySpace(*dbDir)
	if err != nil {
		return fmt.Errorf("load key space: %w", err)
	}
	isNotFound := func(err error) bool { return errors.Is(err, db.ErrNotFound) }
	gen := loadgen.New(engine, isNotFound, keys)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	fmt.Printf("load: rate=%d workers=%d mix=%s dist=%s order=%s value=%dB keys=%+v\n",
		cfg.Rate, *workers, cfg.MixString(), cfg.Dist, cfg.Order, cfg.ValueSize, keys)
	if err := gen.Start(cfg, *workers); err != nil {
		return err
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	var prevOps uint64
	prevAt := time.Now()
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case now := <-ticker.C:
			s := gen.Stats()
			ops := s.TotalOps()
			rate := float64(ops-prevOps) / now.Sub(prevAt).Seconds()
			prevOps, prevAt = ops, now
			printStats(s, rate, engine)
		}
	}

	// Stop the world: once Stop returns no Put/Delete is in flight and every
	// write is fsynced in the WAL, so the directory is safe to copy.
	gen.Stop()
	if err := loadgen.SaveKeySpace(*dbDir, gen.KeySpace()); err != nil {
		return fmt.Errorf("save key space: %w", err)
	}
	s := gen.Stats()
	fmt.Printf("\nstopped after %s: %d ops (%.0f ops/s)\n",
		s.Elapsed.Round(time.Millisecond), s.TotalOps(), float64(s.TotalOps())/s.Elapsed.Seconds())
	printStats(s, 0, engine)

	if *save != "" {
		info, err := checkpoint.Save(*dbDir, root, *save)
		if err != nil {
			return err
		}
		fmt.Printf("saved checkpoint %q (%d files, %s)\n", info.Name, info.Files, humanBytes(info.Bytes))
	}
	if s.Corrupt > 0 {
		return fmt.Errorf("%d gets returned another key's value", s.Corrupt)
	}
	return nil
}

func printStats(s loadgen.Stats, rate float64, engine *db.DB) {
	ins, upd, del, get := s.Ops["insert"], s.Ops["update"], s.Ops["delete"], s.Ops["get"]
	var errs uint64
	for _, o := range s.Ops {
		errs += o.Errors
	}

	var levels strings.Builder
	for i, files := range engine.Manifest().Current().Levels {
		fmt.Fprintf(&levels, " L%d=%d", i, len(files))
	}

	fmt.Printf("[%7.1fs] %6.0f ops/s | ins %d upd %d del %d get %d (miss %d) | put avg %s max %s | get avg %s | mem=%d%s",
		s.Elapsed.Seconds(), rate,
		ins.Count, upd.Count, del.Count, get.Count, s.NotFound,
		roundLatency(ins.AvgLatency()), roundLatency(max(ins.MaxLatency, upd.MaxLatency)), roundLatency(get.AvgLatency()),
		engine.Memtable().Len(), levels.String())
	if errs > 0 || s.Corrupt > 0 {
		fmt.Printf(" | errors %d corrupt %d", errs, s.Corrupt)
	}
	fmt.Println()
	if s.LastError != "" {
		fmt.Printf("  last error: %s\n", s.LastError)
	}
}

func roundLatency(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(100 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

func saveCmd(args []string) error {
	fs := flag.NewFlagSet("save", flag.ExitOnError)
	dbDir, ckptDir := dirFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: loadgen save [-db dir] <name>")
	}
	info, err := checkpoint.Save(*dbDir, checkpointRoot(*dbDir, *ckptDir), fs.Arg(0))
	if err != nil {
		return err
	}
	fmt.Printf("saved checkpoint %q (%d files, %s)\n", info.Name, info.Files, humanBytes(info.Bytes))
	return nil
}

func restoreCmd(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	dbDir, ckptDir := dirFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: loadgen restore [-db dir] <name>")
	}
	if err := checkpoint.Restore(checkpointRoot(*dbDir, *ckptDir), fs.Arg(0), *dbDir); err != nil {
		return err
	}
	fmt.Printf("restored checkpoint %q into %s\n", fs.Arg(0), *dbDir)
	return nil
}

func lsCmd(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	dbDir, ckptDir := dirFlags(fs)
	fs.Parse(args)
	root := checkpointRoot(*dbDir, *ckptDir)
	infos, err := checkpoint.List(root)
	if err != nil {
		return err
	}
	if len(infos) == 0 {
		fmt.Printf("no checkpoints in %s\n", root)
		return nil
	}
	for _, info := range infos {
		keys, _ := loadgen.LoadKeySpace(filepath.Join(root, info.Name))
		fmt.Printf("%-24s %s  %4d files  %9s  keys seq=%d scattered=%d\n",
			info.Name, info.Created.Format("2006-01-02 15:04:05"), info.Files, humanBytes(info.Bytes),
			keys.Sequential, keys.Scattered)
	}
	return nil
}

func rmCmd(args []string) error {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	dbDir, ckptDir := dirFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: loadgen rm [-checkpoints dir] <name>")
	}
	if err := checkpoint.Remove(checkpointRoot(*dbDir, *ckptDir), fs.Arg(0)); err != nil {
		return err
	}
	fmt.Printf("removed checkpoint %q\n", fs.Arg(0))
	return nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
