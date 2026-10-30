// Command gtsdb runs the database as an HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/server"
	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

func main() {
	addr := flag.String("addr", ":9201", "listen address")
	dir := flag.String("data", "data", "data directory (WAL and checkpoints); empty for in-memory")
	retention := flag.Duration("retention", 6*time.Hour, "drop samples older than this")
	ckptEvery := flag.Duration("checkpoint-interval", 10*time.Minute, "how often to apply retention and checkpoint")
	syncWAL := flag.Bool("sync-wal", false, "fsync the WAL on every write request")
	flag.Parse()

	db, err := tsdb.Open(tsdb.Options{Dir: *dir, SyncWAL: *syncWAL})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	r := db.Recovery()
	st := db.Stats()
	log.Printf("recovered %d series, %d samples in %v (checkpoint chunks=%d, wal segments=%d, torn tail=%v)",
		st.Series, st.Samples, r.Duration.Round(time.Millisecond), r.CheckpointChunks, r.WALSegments, r.TornTail)

	maintain := func() {
		removed := db.Truncate(time.Now().Add(-*retention).UnixMilli())
		cs, err := db.Checkpoint()
		if err != nil {
			log.Printf("checkpoint failed: %v", err)
			return
		}
		log.Printf("retention removed %d series; checkpoint %d: %d series, %d chunks, %d bytes",
			removed, cs.Segment, cs.Series, cs.Chunks, cs.Bytes)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		t := time.NewTicker(*ckptEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				maintain()
			}
		}
	}()

	srv := &http.Server{Addr: *addr, Handler: server.New(db, nil), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if err := db.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
