// Package tsdb is the storage engine: an in-memory head of Gorilla-compressed
// chunks per series, an inverted label index, a write-ahead log for
// durability, and checkpoints that bound WAL replay time.
package tsdb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/chunk"
	"github.com/ShryukGrandhi/gorilla-tsdb/index"
	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
	"github.com/ShryukGrandhi/gorilla-tsdb/wal"
)

const stripeCount = 256

// Options configures a DB.
type Options struct {
	// Dir holds the WAL and checkpoints. Empty means purely in-memory.
	Dir string
	// ChunkSamples is the number of samples per chunk before a new one is cut.
	ChunkSamples int
	// SyncWAL fsyncs the WAL on every commit.
	SyncWAL bool
	// WALSegmentSize is the WAL segment rotation size in bytes.
	WALSegmentSize int64
}

// Sample is one timestamped value. Timestamps are milliseconds.
type Sample struct {
	T int64
	V float64
}

// Series is a materialised query result.
type Series struct {
	Labels  labels.Labels
	Samples []Sample
}

// ErrEmptyLabels is returned when appending a series without labels.
var ErrEmptyLabels = errors.New("tsdb: empty label set")

type memSeries struct {
	mu      sync.Mutex
	ref     index.SeriesRef
	lset    labels.Labels
	chunks  []*chunk.XOR
	lastT   int64
	deleted bool
}

func (s *memSeries) append(t int64, v float64, chunkSamples int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t <= s.lastT {
		return false
	}
	var head *chunk.XOR
	if n := len(s.chunks); n > 0 {
		head = s.chunks[n-1]
	}
	if head == nil || head.ReadOnly() || head.NumSamples() >= chunkSamples {
		head = chunk.NewXOR()
		s.chunks = append(s.chunks, head)
	}
	if err := head.Append(t, v); err != nil {
		return false
	}
	s.lastT = t
	return true
}

type stripe struct {
	mu     sync.RWMutex
	byHash map[uint64][]*memSeries
	byRef  map[index.SeriesRef]*memSeries
}

// DB is a time-series database instance. It is safe for concurrent use.
type DB struct {
	opts     Options
	wal      *wal.WAL
	postings *index.Postings
	stripes  [stripeCount]stripe
	nextRef  atomic.Uint64

	// gcMu orders commits against WAL cuts and series garbage collection:
	// commits hold it shared, Checkpoint's cut and Truncate hold it exclusively.
	gcMu sync.RWMutex

	appended   atomic.Uint64
	outOfOrder atomic.Uint64

	ckptMu sync.Mutex
	recov  RecoveryStats
}

// RecoveryStats describes what Open loaded from disk.
type RecoveryStats struct {
	CheckpointSeries int
	CheckpointChunks int
	WALSegments      int
	WALRecords       int
	WALSamples       int
	TornTail         bool
	Duration         time.Duration
}

// Open creates or recovers a database.
func Open(opts Options) (*DB, error) {
	if opts.ChunkSamples <= 0 {
		opts.ChunkSamples = 120
	}
	db := &DB{opts: opts, postings: index.NewPostings()}
	for i := range db.stripes {
		db.stripes[i].byHash = make(map[uint64][]*memSeries)
		db.stripes[i].byRef = make(map[index.SeriesRef]*memSeries)
	}
	if opts.Dir == "" {
		return db, nil
	}
	start := time.Now()
	walDir := filepath.Join(opts.Dir, "wal")
	from := 0
	ckpt, ckptIdx, err := latestCheckpoint(opts.Dir)
	if err != nil {
		return nil, err
	}
	if ckpt != "" {
		if err := db.loadCheckpoint(ckpt); err != nil {
			return nil, fmt.Errorf("load checkpoint: %w", err)
		}
		from = ckptIdx
	}
	if err := db.replayWAL(walDir, from); err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}
	w, err := wal.Open(walDir, wal.Options{SegmentSize: opts.WALSegmentSize, Sync: opts.SyncWAL})
	if err != nil {
		return nil, err
	}
	db.wal = w
	if ckpt != "" {
		// Segments older than the checkpoint can linger after a crash.
		if err := w.TruncateBefore(ckptIdx); err != nil {
			return nil, err
		}
	}
	db.recov.Duration = time.Since(start)
	return db, nil
}

// Recovery returns statistics about the most recent Open.
func (db *DB) Recovery() RecoveryStats { return db.recov }

// Close flushes and closes the WAL.
func (db *DB) Close() error {
	if db.wal != nil {
		return db.wal.Close()
	}
	return nil
}

func (db *DB) hashStripe(h uint64) *stripe { return &db.stripes[h%stripeCount] }
func (db *DB) refStripe(r index.SeriesRef) *stripe {
	return &db.stripes[uint64(r)%stripeCount]
}

func (db *DB) getByRef(ref index.SeriesRef) *memSeries {
	st := db.refStripe(ref)
	st.mu.RLock()
	s := st.byRef[ref]
	st.mu.RUnlock()
	return s
}

// getOrCreate returns the series for lset, creating it if needed. A new
// series is written to the WAL before any other goroutine can observe it, so
// a sample record can never precede its series record in the log.
func (db *DB) getOrCreate(lset labels.Labels) (*memSeries, error) {
	h := lset.Hash()
	st := db.hashStripe(h)
	st.mu.RLock()
	for _, s := range st.byHash[h] {
		if labels.Equal(s.lset, lset) {
			st.mu.RUnlock()
			return s, nil
		}
	}
	st.mu.RUnlock()

	st.mu.Lock()
	for _, s := range st.byHash[h] {
		if labels.Equal(s.lset, lset) {
			st.mu.Unlock()
			return s, nil
		}
	}
	ref := index.SeriesRef(db.nextRef.Add(1))
	s := &memSeries{ref: ref, lset: lset.Copy(), lastT: math.MinInt64}
	if db.wal != nil {
		if err := db.wal.Log(encodeSeries(nil, ref, s.lset)); err != nil {
			st.mu.Unlock()
			return nil, err
		}
	}
	st.byHash[h] = append(st.byHash[h], s)
	st.mu.Unlock()
	db.insertRef(s)
	return s, nil
}

func (db *DB) insertRef(s *memSeries) {
	rs := db.refStripe(s.ref)
	rs.mu.Lock()
	rs.byRef[s.ref] = s
	rs.mu.Unlock()
	db.postings.Add(s.ref, s.lset)
}

// createWithRef is used during recovery, where refs come from disk.
func (db *DB) createWithRef(ref index.SeriesRef, lset labels.Labels) *memSeries {
	if s := db.getByRef(ref); s != nil {
		return s
	}
	h := lset.Hash()
	st := db.hashStripe(h)
	s := &memSeries{ref: ref, lset: lset, lastT: math.MinInt64}
	st.mu.Lock()
	st.byHash[h] = append(st.byHash[h], s)
	st.mu.Unlock()
	db.insertRef(s)
	for {
		cur := db.nextRef.Load()
		if uint64(ref) <= cur || db.nextRef.CompareAndSwap(cur, uint64(ref)) {
			break
		}
	}
	return s
}

// Appender batches samples and commits them as one WAL record.
// An Appender must not be used from multiple goroutines.
type Appender struct {
	db      *DB
	series  []*memSeries
	lsets   []labels.Labels
	samples []refSample
	buf     []byte
}

// Appender returns a new appender.
func (db *DB) Appender() *Appender { return &Appender{db: db} }

// Add buffers a sample for the series identified by lset.
func (a *Appender) Add(lset labels.Labels, t int64, v float64) error {
	if len(lset) == 0 {
		return ErrEmptyLabels
	}
	s, err := a.db.getOrCreate(lset)
	if err != nil {
		return err
	}
	a.series = append(a.series, s)
	a.lsets = append(a.lsets, lset)
	a.samples = append(a.samples, refSample{ref: s.ref, t: t, v: v})
	return nil
}

// CommitResult reports how many samples a commit stored.
type CommitResult struct {
	Accepted   int
	OutOfOrder int
}

// Commit writes the batch to the WAL and then to the in-memory head.
// Samples whose timestamp is not newer than the series' last sample are
// dropped and counted as out of order.
func (a *Appender) Commit() (CommitResult, error) {
	var res CommitResult
	if len(a.samples) == 0 {
		return res, nil
	}
	db := a.db
	db.gcMu.RLock()
	// A series may have been garbage-collected by Truncate between Add and
	// Commit; re-resolve it so the WAL never references a dead ref.
	for i, s := range a.series {
		s.mu.Lock()
		dead := s.deleted
		s.mu.Unlock()
		if dead {
			fresh, err := db.getOrCreate(a.lsets[i])
			if err != nil {
				db.gcMu.RUnlock()
				return res, err
			}
			a.series[i] = fresh
			a.samples[i].ref = fresh.ref
		}
	}
	if db.wal != nil {
		a.buf = encodeSamples(a.buf, a.samples)
		if err := db.wal.Log(a.buf); err != nil {
			db.gcMu.RUnlock()
			return res, err
		}
	}
	for i, s := range a.series {
		if s.append(a.samples[i].t, a.samples[i].v, db.opts.ChunkSamples) {
			res.Accepted++
		} else {
			res.OutOfOrder++
		}
	}
	db.gcMu.RUnlock()
	db.appended.Add(uint64(res.Accepted))
	db.outOfOrder.Add(uint64(res.OutOfOrder))
	a.Rollback()
	return res, nil
}

// Rollback discards buffered samples.
func (a *Appender) Rollback() {
	a.series = a.series[:0]
	a.lsets = a.lsets[:0]
	a.samples = a.samples[:0]
}

// Select returns all series matching ms with their samples in [mint, maxt],
// sorted by label set. Series without samples in range are omitted.
func (db *DB) Select(mint, maxt int64, ms ...*labels.Matcher) []Series {
	refs := db.postings.Select(ms...)
	out := make([]Series, 0, len(refs))
	for _, ref := range refs {
		s := db.getByRef(ref)
		if s == nil {
			continue
		}
		var samples []Sample
		s.mu.Lock()
		for _, c := range s.chunks {
			if c.NumSamples() == 0 || c.MaxT() < mint || c.MinT() > maxt {
				continue
			}
			it := c.Iterator()
			for it.Next() {
				t, v := it.At()
				if t < mint {
					continue
				}
				if t > maxt {
					break
				}
				samples = append(samples, Sample{t, v})
			}
		}
		lset := s.lset
		s.mu.Unlock()
		if len(samples) > 0 {
			out = append(out, Series{Labels: lset, Samples: samples})
		}
	}
	sort.Slice(out, func(i, j int) bool { return labels.Compare(out[i].Labels, out[j].Labels) < 0 })
	return out
}

// LabelNames returns all label names in the index.
func (db *DB) LabelNames() []string { return db.postings.LabelNames() }

// LabelValues returns all values of a label.
func (db *DB) LabelValues(name string) []string { return db.postings.LabelValues(name) }

// Truncate drops chunks whose samples are all older than mint and removes
// series left without data. It returns the number of series removed.
func (db *DB) Truncate(mint int64) int {
	db.gcMu.Lock()
	defer db.gcMu.Unlock()
	dead := map[index.SeriesRef]labels.Labels{}
	for i := range db.stripes {
		st := &db.stripes[i]
		st.mu.RLock()
		for _, s := range st.byRef {
			s.mu.Lock()
			keep := s.chunks[:0]
			for _, c := range s.chunks {
				if c.MaxT() >= mint {
					keep = append(keep, c)
				}
			}
			for j := len(keep); j < len(s.chunks); j++ {
				s.chunks[j] = nil
			}
			s.chunks = keep
			if len(s.chunks) == 0 {
				s.deleted = true
				dead[s.ref] = s.lset
			}
			s.mu.Unlock()
		}
		st.mu.RUnlock()
	}
	for ref, ls := range dead {
		rs := db.refStripe(ref)
		rs.mu.Lock()
		delete(rs.byRef, ref)
		rs.mu.Unlock()
		h := ls.Hash()
		hs := db.hashStripe(h)
		hs.mu.Lock()
		list := hs.byHash[h]
		for j, s := range list {
			if s.ref == ref {
				list = append(list[:j], list[j+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(hs.byHash, h)
		} else {
			hs.byHash[h] = list
		}
		hs.mu.Unlock()
	}
	db.postings.Delete(dead)
	return len(dead)
}

// Stats summarises the head.
type Stats struct {
	Series         int
	Chunks         int
	Samples        int
	ChunkBytes     int
	Appended       uint64
	OutOfOrder     uint64
	BytesPerSample float64
}

// Stats walks the head and returns current totals.
func (db *DB) Stats() Stats {
	var st Stats
	for i := range db.stripes {
		s := &db.stripes[i]
		s.mu.RLock()
		for _, ms := range s.byRef {
			ms.mu.Lock()
			st.Series++
			for _, c := range ms.chunks {
				st.Chunks++
				st.Samples += c.NumSamples()
				st.ChunkBytes += c.Size()
			}
			ms.mu.Unlock()
		}
		s.mu.RUnlock()
	}
	st.Appended = db.appended.Load()
	st.OutOfOrder = db.outOfOrder.Load()
	if st.Samples > 0 {
		st.BytesPerSample = float64(st.ChunkBytes) / float64(st.Samples)
	}
	return st
}

func (db *DB) replayWAL(dir string, from int) error {
	var buf []refSample
	st, err := wal.Replay(dir, from, func(rec []byte) error {
		if len(rec) == 0 {
			return errBadRecord
		}
		switch rec[0] {
		case recSeries:
			ref, ls, err := decodeSeries(rec)
			if err != nil {
				return err
			}
			db.createWithRef(ref, ls)
		case recSamples:
			var err error
			buf, err = decodeSamples(rec, buf)
			if err != nil {
				return err
			}
			for _, s := range buf {
				ms := db.getByRef(s.ref)
				if ms == nil {
					db.outOfOrder.Add(1)
					continue
				}
				if ms.append(s.t, s.v, db.opts.ChunkSamples) {
					db.recov.WALSamples++
				}
			}
		default:
			return fmt.Errorf("unknown record type %d", rec[0])
		}
		return nil
	})
	db.recov.WALSegments = st.Segments
	db.recov.WALRecords = st.Records
	db.recov.TornTail = st.TornTailDrop
	return err
}

const checkpointPrefix = "checkpoint."

func latestCheckpoint(dir string) (string, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, nil
		}
		return "", 0, err
	}
	best, bestIdx := "", -1
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, checkpointPrefix) {
			continue
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(name, checkpointPrefix))
		if err != nil {
			continue // e.g. an abandoned .tmp file
		}
		if idx > bestIdx {
			best, bestIdx = filepath.Join(dir, name), idx
		}
	}
	return best, bestIdx, nil
}
