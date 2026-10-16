// Command viz serves a live visualization of an Amethyst DB.
//
// It owns the DB, the load generator and checkpoints, and streams a metadata
// snapshot of the LSM tree to the browser over Server-Sent Events. The server
// polls the engine on a ticker and only sends a frame when the snapshot
// changed; the engine itself has no hooks or event plumbing.
//
//	go run ./cmd/viz -db data/adb          # then open http://127.0.0.1:7070
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"amethyst/internal/checkpoint"
	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/loadgen"
)

//go:embed static
var staticFiles embed.FS

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "listen address (keep it on localhost: there is no auth)")
	dbDir := flag.String("db", "data/adb", "DB directory")
	ckptDir := flag.String("checkpoints", "", "checkpoint directory (default: <db parent>/checkpoints)")
	tick := flag.Duration("tick", 100*time.Millisecond, "how often to sample the DB")
	// Fatter than the engine defaults so sustained load doesn't hammer the
	// filesystem: fewer WAL rotations/SSTables and fewer fsyncs per second.
	flushThreshold := flag.Int("flush-threshold", 1024, "memtable entries before flush (and WAL rotation)")
	batchSize := flag.Int("batch-size", 256, "max writes per group commit (fsync); capped in practice by concurrent writers")
	batchTimeout := flag.Duration("batch-timeout", 20*time.Millisecond, "max wait to fill a group commit; adds up to this much write latency")
	flag.Parse()

	if *ckptDir == "" {
		*ckptDir = filepath.Join(filepath.Dir(filepath.Clean(*dbDir)), "checkpoints")
	}

	// Engine logging (every Get prints) would flood the terminal under load.
	common.LoggingEnabled = false

	s := &server{
		dbDir:    *dbDir,
		ckptRoot: *ckptDir,
		dbOpts: []db.Option{
			db.WithDBPath(*dbDir),
			db.WithMemtableFlushThreshold(*flushThreshold),
			db.WithMaxBatchSize(*batchSize),
			db.WithBatchTimeout(*batchTimeout),
		},
		hub: newHub(),
	}
	if err := s.open(); err != nil {
		log.Fatalf("open %s: %v", *dbDir, err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go s.sample(ctx, *tick)

	srv := &http.Server{
		Addr:    *addr,
		Handler: s.routes(),
		// SSE handlers watch the request context, so cancelling this lets
		// Shutdown finish instead of waiting on open streams.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		srv.Shutdown(shutdownCtx)
	}()

	fmt.Printf("amethyst viz: http://%s  (db=%s, checkpoints=%s)\n", *addr, *dbDir, *ckptDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	s.close()
}

// server owns the DB and the load generator. mu guards swapping them out
// (restore/clear); everything else is safe to call concurrently.
type server struct {
	dbDir    string
	ckptRoot string
	dbOpts   []db.Option
	hub      *hub

	mu     sync.RWMutex
	engine *db.DB
	gen    *loadgen.Generator
	epoch  int
}

func (s *server) open() error {
	engine, err := db.Open(s.dbOpts...)
	if err != nil {
		return err
	}
	keys, err := loadgen.LoadKeySpace(s.dbDir)
	if err != nil {
		return err
	}
	cfg := loadgen.DefaultConfig
	if s.gen != nil {
		cfg = s.gen.Config() // keep the UI's workload settings across restores
	}
	s.engine = engine
	s.gen = loadgen.New(engine, isNotFound, keys)
	s.gen.SetConfig(cfg)
	s.epoch++
	return nil
}

func isNotFound(err error) bool { return errors.Is(err, db.ErrNotFound) }

// stopLoad stops the generator and persists its key space. Caller holds mu.
func (s *server) stopLoad() error {
	s.gen.Stop()
	return loadgen.SaveKeySpace(s.dbDir, s.gen.KeySpace())
}

func (s *server) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stopLoad(); err != nil {
		log.Printf("save key space: %v", err)
	}
	s.engine.Close()
}

// replaceDB stops the world, lets swap rewrite the DB directory, and reopens it.
func (s *server) replaceDB(swap func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stopLoad(); err != nil {
		return err
	}
	// Close is still a stub (see db.go), so the old engine is simply abandoned:
	// it has no work in flight and nothing references it after the swap.
	s.engine.Close()
	if err := swap(); err != nil {
		return err
	}
	return s.open()
}

// sample polls the DB every tick and publishes the snapshot when it changed.
func (s *server) sample(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	tables := tableCache{}
	epoch := 0
	var last []byte
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		s.mu.RLock()
		if s.epoch != epoch {
			tables, epoch = tableCache{}, s.epoch
		}
		snap := buildSnapshot(s.engine, s.gen, s.epoch, tables)
		s.mu.RUnlock()

		b, err := json.Marshal(snap)
		if err != nil {
			log.Printf("marshal snapshot: %v", err)
			continue
		}
		if bytes.Equal(b, last) {
			continue
		}
		last = b
		s.hub.publish(b)
	}
}

// ============================================================================
// HTTP
// ============================================================================

func (s *server) routes() http.Handler {
	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("POST /api/load/start", s.handleLoadStart)
	mux.HandleFunc("POST /api/load/stop", s.handleLoadStop)
	mux.HandleFunc("POST /api/load/config", s.handleLoadConfig)
	mux.HandleFunc("POST /api/get", s.handleGet)
	mux.HandleFunc("GET /api/checkpoints", s.handleCheckpointList)
	mux.HandleFunc("POST /api/checkpoints/save", s.handleCheckpointSave)
	mux.HandleFunc("POST /api/checkpoints/restore", s.handleCheckpointRestore)
	mux.HandleFunc("POST /api/checkpoints/delete", s.handleCheckpointDelete)
	mux.HandleFunc("POST /api/clear", s.handleClear)
	return mux
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch, latest := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)
	if latest != nil {
		fmt.Fprintf(w, "data: %s\n\n", latest)
	}
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": ping\n\n")
		case b := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		flusher.Flush()
	}
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(s.hub.last())
}

type loadRequest struct {
	Config  loadgen.Config `json:"config"`
	Workers int            `json:"workers"`
}

func (s *server) handleLoadStart(w http.ResponseWriter, r *http.Request) {
	var req loadRequest
	if !decode(w, r, &req) {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.gen.Start(req.Config, req.Workers); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]bool{"running": true})
}

func (s *server) handleLoadStop(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.stopLoad(); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]bool{"running": false})
}

func (s *server) handleLoadConfig(w http.ResponseWriter, r *http.Request) {
	var req loadRequest
	if !decode(w, r, &req) {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.gen.SetConfig(req.Config); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, req.Config)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if !decode(w, r, &req) {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := time.Now()
	val, err := s.engine.Get([]byte(req.Key))
	elapsed := time.Since(start)
	resp := map[string]any{"key": req.Key, "micros": elapsed.Microseconds(), "found": err == nil}
	switch {
	case err == nil:
		resp["value"] = string(val)
	case isNotFound(err):
	default:
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, resp)
}

func (s *server) handleCheckpointList(w http.ResponseWriter, r *http.Request) {
	infos, err := checkpoint.List(s.ckptRoot)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	if infos == nil {
		infos = []checkpoint.Info{}
	}
	writeJSON(w, infos)
}

type nameRequest struct {
	Name string `json:"name"`
}

func (s *server) handleCheckpointSave(w http.ResponseWriter, r *http.Request) {
	var req nameRequest
	if !decode(w, r, &req) {
		return
	}
	// Exclusive lock: no Get from the UI runs while the directory is copied.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen.Running() {
		httpError(w, http.StatusConflict, errors.New("stop the load before saving a checkpoint"))
		return
	}
	if err := loadgen.SaveKeySpace(s.dbDir, s.gen.KeySpace()); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	info, err := checkpoint.Save(s.dbDir, s.ckptRoot, req.Name)
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, info)
}

func (s *server) handleCheckpointRestore(w http.ResponseWriter, r *http.Request) {
	var req nameRequest
	if !decode(w, r, &req) {
		return
	}
	err := s.replaceDB(func() error { return checkpoint.Restore(s.ckptRoot, req.Name, s.dbDir) })
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]string{"restored": req.Name})
}

func (s *server) handleCheckpointDelete(w http.ResponseWriter, r *http.Request) {
	var req nameRequest
	if !decode(w, r, &req) {
		return
	}
	if err := checkpoint.Remove(s.ckptRoot, req.Name); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]string{"deleted": req.Name})
}

func (s *server) handleClear(w http.ResponseWriter, r *http.Request) {
	if err := s.replaceDB(func() error { return os.RemoveAll(s.dbDir) }); err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]bool{"cleared": true})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		httpError(w, http.StatusBadRequest, fmt.Errorf("bad request body: %w", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// ============================================================================
// Snapshot fan-out
// ============================================================================

// hub fans the latest snapshot out to SSE clients. Each client has a one-slot
// mailbox; a slow client just skips to the newest frame.
type hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
	latest  []byte
}

func newHub() *hub {
	return &hub{clients: make(map[chan []byte]struct{})}
}

func (h *hub) subscribe() (chan []byte, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan []byte, 1)
	h.clients[ch] = struct{}{}
	return ch, h.latest
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, ch)
}

func (h *hub) last() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.latest
}

func (h *hub) publish(b []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.latest = b
	for ch := range h.clients {
		select {
		case <-ch: // drop the stale frame
		default:
		}
		ch <- b
	}
}
