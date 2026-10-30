package tsdb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
	"github.com/ShryukGrandhi/gorilla-tsdb/wal"
)

var eq = func(n, v string) *labels.Matcher { return labels.MustMatcher(labels.MatchEqual, n, v) }

func mustOpen(t *testing.T, opts Options) *DB {
	t.Helper()
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func appendN(t *testing.T, db *DB, ls labels.Labels, from, n int, step int64) {
	t.Helper()
	app := db.Appender()
	for i := from; i < from+n; i++ {
		if err := app.Add(ls, int64(i)*step, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := app.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendSelectAcrossChunks(t *testing.T) {
	db := mustOpen(t, Options{ChunkSamples: 10})
	a := labels.FromStrings("__name__", "cpu", "host", "a")
	b := labels.FromStrings("__name__", "cpu", "host", "b")
	appendN(t, db, a, 0, 95, 1000)
	appendN(t, db, b, 0, 5, 1000)

	got := db.Select(20_000, 49_000, eq("__name__", "cpu"))
	if len(got) != 1 || !labels.Equal(got[0].Labels, a) {
		t.Fatalf("expected only host=a in range, got %d series", len(got))
	}
	if n := len(got[0].Samples); n != 30 {
		t.Fatalf("got %d samples, want 30", n)
	}
	for i, s := range got[0].Samples {
		if s.T != int64(20+i)*1000 || s.V != float64(20+i) {
			t.Fatalf("sample %d = %+v", i, s)
		}
	}
	st := db.Stats()
	if st.Series != 2 || st.Samples != 100 || st.Chunks != 11 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestOutOfOrderDropped(t *testing.T) {
	db := mustOpen(t, Options{})
	ls := labels.FromStrings("__name__", "x")
	app := db.Appender()
	_ = app.Add(ls, 10, 1)
	_ = app.Add(ls, 20, 2)
	_ = app.Add(ls, 15, 3)
	_ = app.Add(ls, 20, 4)
	res, err := app.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 || res.OutOfOrder != 2 {
		t.Fatalf("result = %+v", res)
	}
}

func TestEmptyLabelsRejected(t *testing.T) {
	db := mustOpen(t, Options{})
	if err := db.Appender().Add(nil, 1, 1); err != ErrEmptyLabels {
		t.Fatalf("got %v", err)
	}
}

func snapshot(db *DB) map[string][]Sample {
	out := map[string][]Sample{}
	for _, s := range db.Select(-1<<62, 1<<62) {
		out[s.Labels.String()] = s.Samples
	}
	return out
}

func equalSnapshots(t *testing.T, got, want map[string][]Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("series count %d want %d", len(got), len(want))
	}
	for k, w := range want {
		g := got[k]
		if len(g) != len(w) {
			t.Fatalf("%s: %d samples want %d", k, len(g), len(w))
		}
		for i := range w {
			if g[i] != w[i] {
				t.Fatalf("%s sample %d: %+v want %+v", k, i, g[i], w[i])
			}
		}
	}
}

func fill(t *testing.T, db *DB, seed int64, series, perSeries int, startT int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	app := db.Appender()
	for i := 0; i < perSeries; i++ {
		for s := 0; s < series; s++ {
			ls := labels.FromStrings("__name__", "m", "s", fmt.Sprint(s))
			if err := app.Add(ls, startT+int64(i)*15000, rng.NormFloat64()); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := app.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, Options{Dir: dir, ChunkSamples: 50, WALSegmentSize: 4096})
	fill(t, db, 1, 20, 300, 0)
	want := snapshot(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2 := mustOpen(t, Options{Dir: dir, ChunkSamples: 50})
	defer db2.Close()
	equalSnapshots(t, snapshot(db2), want)
	if r := db2.Recovery(); r.WALSamples != 20*300 || r.WALSegments < 2 {
		t.Fatalf("recovery = %+v", r)
	}
}

func TestRecoveryFromCheckpointPlusWAL(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, Options{Dir: dir, ChunkSamples: 50})
	fill(t, db, 1, 10, 200, 0)
	cs, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if cs.Series != 10 || cs.Chunks != 40 {
		t.Fatalf("checkpoint stats = %+v", cs)
	}
	fill(t, db, 2, 15, 100, 200*15000) // 5 new series + more samples
	want := snapshot(db)
	_ = db.Close()

	segs, _ := wal.Segments(filepath.Join(dir, "wal"))
	if segs[0] < cs.Segment {
		t.Fatalf("segments before checkpoint not deleted: %v", segs)
	}
	db2 := mustOpen(t, Options{Dir: dir, ChunkSamples: 50})
	equalSnapshots(t, snapshot(db2), want)
	r := db2.Recovery()
	if r.CheckpointSeries != 10 || r.WALSamples != 15*100 {
		t.Fatalf("recovery = %+v", r)
	}
	// The recovered head must accept new appends after read-only chunks.
	appendN(t, db2, labels.FromStrings("__name__", "m", "s", "0"), 1000, 3, 15000)
	_ = db2.Close()

	// A second checkpoint replaces the first.
	db3 := mustOpen(t, Options{Dir: dir, ChunkSamples: 50})
	if _, err := db3.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = db3.Close()
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly one checkpoint file, found %d", n)
	}
}

func TestRecoverySurvivesTornTail(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, Options{Dir: dir})
	fill(t, db, 1, 3, 10, 0)
	_ = db.Close()
	segs, _ := wal.Segments(filepath.Join(dir, "wal"))
	path := wal.SegmentName(filepath.Join(dir, "wal"), segs[len(segs)-1])
	info, _ := os.Stat(path)
	_ = os.Truncate(path, info.Size()-3)
	db2 := mustOpen(t, Options{Dir: dir})
	defer db2.Close()
	r := db2.Recovery()
	if !r.TornTail || r.WALSamples != 27 {
		t.Fatalf("recovery = %+v", r)
	}
}

func TestTruncateRemovesOldDataAndSeries(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, Options{Dir: dir, ChunkSamples: 10})
	old := labels.FromStrings("__name__", "old")
	live := labels.FromStrings("__name__", "live")
	appendN(t, db, old, 0, 20, 1000)
	appendN(t, db, live, 0, 100, 1000)
	removed := db.Truncate(50_000)
	if removed != 1 {
		t.Fatalf("removed %d series", removed)
	}
	if got := db.Select(0, 1<<40, eq("__name__", "old")); len(got) != 0 {
		t.Fatal("old series still selectable")
	}
	got := db.Select(0, 1<<40, eq("__name__", "live"))
	if len(got) != 1 || got[0].Samples[0].T != 50_000 {
		t.Fatalf("live series first sample = %+v", got[0].Samples[0])
	}
	if vals := db.LabelValues("__name__"); len(vals) != 1 {
		t.Fatalf("label values = %v", vals)
	}
	// An appender that resolved the series before truncation still works.
	app := db.Appender()
	_ = app.Add(old, 1, 1)
	db.Truncate(50_000)
	_ = app.Add(old, 200_000, 7)
	if _, err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	db2 := mustOpen(t, Options{Dir: dir, ChunkSamples: 10})
	defer db2.Close()
	got = db2.Select(0, 1<<40, eq("__name__", "old"))
	if len(got) != 1 || len(got[0].Samples) != 2 {
		t.Fatalf("re-created series after recovery: %+v", got)
	}
}

func TestConcurrentAppendAndQuery(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, Options{Dir: dir})
	const writers, perWriter, seriesPer = 8, 200, 25
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			app := db.Appender()
			for i := 0; i < perWriter; i++ {
				for s := 0; s < seriesPer; s++ {
					ls := labels.FromStrings("__name__", "req", "w", fmt.Sprint(w), "s", fmt.Sprint(s))
					if err := app.Add(ls, int64(i)*1000, float64(i)); err != nil {
						t.Error(err)
						return
					}
				}
				if _, err := app.Commit(); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	var qwg sync.WaitGroup
	qwg.Add(2)
	go func() {
		defer qwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				db.Select(0, 1<<40, eq("__name__", "req"), eq("w", "3"))
			}
		}
	}()
	go func() {
		defer qwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := db.Checkpoint(); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	wg.Wait()
	close(stop)
	qwg.Wait()
	st := db.Stats()
	if st.Series != writers*seriesPer || st.Samples != writers*perWriter*seriesPer || st.OutOfOrder != 0 {
		t.Fatalf("stats = %+v", st)
	}
	// Checkpoints raced with writers; recovery must still see every sample.
	want := snapshot(db)
	_ = db.Close()
	db2 := mustOpen(t, Options{Dir: dir})
	defer db2.Close()
	equalSnapshots(t, snapshot(db2), want)
}
