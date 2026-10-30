// Command tsbench generates a synthetic monitoring workload and measures
// compression, ingest throughput, query latency and crash-recovery time.
// All output numbers come from this run; nothing is hard-coded.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/chunk"
	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
	"github.com/ShryukGrandhi/gorilla-tsdb/query"
	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

const scrapeMs = 15_000

// gen produces successive values of one synthetic series.
type gen func(rng *rand.Rand) float64

func constant() gen { return func(*rand.Rand) float64 { return 1 } }

func counterInt(ratePerScrape float64) gen {
	v := 0.0
	return func(r *rand.Rand) float64 {
		v += math.Max(0, math.Round(ratePerScrape+r.NormFloat64()*math.Sqrt(ratePerScrape)))
		return v
	}
}

func counterFloat() gen {
	v := 0.0
	return func(r *rand.Rand) float64 {
		v += 15 * (0.2 + 0.1*r.Float64()) // CPU seconds at 20-30% utilisation
		return v
	}
}

func gaugeInt(start, step float64) gen {
	v := start
	return func(r *rand.Rand) float64 {
		v = math.Max(0, v+math.Round(r.NormFloat64()*step))
		return v
	}
}

func gaugeDecimals(start, step float64, decimals int) gen {
	v := start
	p := math.Pow(10, float64(decimals))
	return func(r *rand.Rand) float64 {
		v += r.NormFloat64() * step
		return math.Round(v*p) / p
	}
}

func randomFloat() gen { return func(r *rand.Rand) float64 { return r.Float64() * 1000 } }

type metricDef struct {
	name  string
	extra []string
	mk    func() gen
}

var hostMetrics = []metricDef{
	{"up", nil, constant},
	{"http_requests_total", []string{"code", "200"}, func() gen { return counterInt(300) }},
	{"http_requests_total", []string{"code", "500"}, func() gen { return counterInt(0.2) }},
	{"cpu_seconds_total", nil, counterFloat},
	{"memory_bytes", nil, func() gen { return gaugeInt(4<<30, 4096) }},
	{"disk_free_bytes", nil, func() gen { return gaugeInt(200<<30, 1<<20) }},
	{"queue_depth", nil, func() gen { return gaugeInt(10, 2) }},
	{"temperature_celsius", nil, func() gen { return gaugeDecimals(45, 0.3, 1) }},
	{"load1", nil, func() gen { return gaugeDecimals(2, 0.1, 2) }},
	{"latency_seconds", nil, randomFloat},
}

type series struct {
	lset labels.Labels
	next gen
	rng  *rand.Rand
}

func buildSeries(hosts int) []*series {
	var out []*series
	for h := 0; h < hosts; h++ {
		host := fmt.Sprintf("host-%04d", h)
		for i, m := range hostMetrics {
			kv := append([]string{"__name__", m.name, "host", host, "dc", fmt.Sprintf("dc%d", h%4)}, m.extra...)
			out = append(out, &series{
				lset: labels.FromStrings(kv...),
				next: m.mk(),
				rng:  rand.New(rand.NewSource(int64(h*100 + i))),
			})
		}
	}
	return out
}

// jitter models real scrapers, which never land exactly on the interval.
func jitter(r *rand.Rand) int64 { return int64(r.Intn(7) - 3) }

func main() {
	quick := flag.Bool("quick", false, "small workload for CI")
	hosts := flag.Int("hosts", 1000, "simulated hosts (10 series each)")
	hours := flag.Float64("hours", 6, "hours of data at a 15s scrape interval")
	writers := flag.Int("writers", runtime.GOMAXPROCS(0), "concurrent ingest goroutines")
	iters := flag.Int("query-iters", 50, "repetitions per query")
	flag.Parse()
	if *quick {
		*hosts, *hours, *iters = 100, 2, 20
	}
	scrapes := int(*hours * 3600 * 1000 / scrapeMs)

	fmt.Printf("# tsbench\n\n")
	fmt.Printf("go %s, %s/%s, GOMAXPROCS=%d, writers=%d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), *writers)
	fmt.Printf("workload: %d hosts x %d metrics = %d series, %d scrapes (%.1fh @15s) = %d samples\n\n",
		*hosts, len(hostMetrics), *hosts*len(hostMetrics), scrapes, *hours, *hosts*len(hostMetrics)*scrapes)

	compression()
	dir, err := os.MkdirTemp("", "tsbench-*")
	must(err)
	defer os.RemoveAll(dir)
	db := ingest(dir, *hosts, scrapes, *writers)
	queries(db, *hosts, scrapes, *iters)
	recovery(db, dir)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func compression() {
	const n = 120 * 1000 // 1000 full chunks per shape
	shapes := []struct {
		name string
		mk   func() gen
	}{
		{"constant (up)", constant},
		{"integer counter (~300/scrape)", func() gen { return counterInt(300) }},
		{"float counter (cpu seconds)", counterFloat},
		{"integer gauge random walk", func() gen { return gaugeInt(4<<30, 4096) }},
		{"gauge, 1 decimal", func() gen { return gaugeDecimals(45, 0.3, 1) }},
		{"gauge, 2 decimals", func() gen { return gaugeDecimals(2, 0.1, 2) }},
		{"uniform random float64", randomFloat},
	}
	fmt.Printf("## Compression (Gorilla chunks of 120 samples, %d samples per shape)\n\n", n)
	fmt.Printf("Timestamps are 15s apart, either exact or with +/-3ms jitter.\n\n")
	fmt.Printf("| series shape | bytes/sample (exact 15s) | bytes/sample (jittered) | vs. 16-byte raw (jittered) |\n|---|---:|---:|---:|\n")
	encode := func(i int, mk func() gen, withJitter bool) float64 {
		rng := rand.New(rand.NewSource(int64(i)))
		g := mk()
		total := 0
		var c *chunk.XOR
		t := int64(1_700_000_000_000)
		for j := 0; j < n; j++ {
			if c == nil || c.NumSamples() == 120 {
				if c != nil {
					total += c.Size()
				}
				c = chunk.NewXOR()
			}
			t += scrapeMs
			if withJitter {
				t += jitter(rng)
			}
			must(c.Append(t, g(rng)))
		}
		total += c.Size()
		return float64(total) / n
	}
	for i, s := range shapes {
		exact := encode(i, s.mk, false)
		jit := encode(i, s.mk, true)
		fmt.Printf("| %s | %.2f | %.2f | %.1fx |\n", s.name, exact, jit, 16/jit)
	}
	fmt.Println()
}

func ingest(dir string, hosts, scrapes, writers int) *tsdb.DB {
	fmt.Printf("## Ingest\n\n")
	fmt.Printf("Each writer goroutine owns a slice of hosts and commits one batch per scrape.\n\n")
	fmt.Printf("| mode | samples | wall time | samples/sec | head bytes/sample |\n|---|---:|---:|---:|---:|\n")
	var keep *tsdb.DB
	for _, mode := range []string{"in-memory (no WAL)", "WAL, no fsync"} {
		opts := tsdb.Options{}
		if mode != "in-memory (no WAL)" {
			opts.Dir = dir
		}
		db, err := tsdb.Open(opts)
		must(err)
		all := buildSeries(hosts)
		per := (len(all) + writers - 1) / writers
		start := time.Now()
		var wg sync.WaitGroup
		var errOnce sync.Once
		var firstErr error
		for w := 0; w < writers; w++ {
			lo, hi := w*per, min((w+1)*per, len(all))
			if lo >= hi {
				continue
			}
			wg.Add(1)
			go func(part []*series) {
				defer wg.Done()
				app := db.Appender()
				base := int64(1_700_000_000_000)
				for s := 0; s < scrapes; s++ {
					ts := base + int64(s)*scrapeMs
					for _, se := range part {
						if err := app.Add(se.lset, ts+jitter(se.rng), se.next(se.rng)); err != nil {
							errOnce.Do(func() { firstErr = err })
							return
						}
					}
					if _, err := app.Commit(); err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
				}
			}(all[lo:hi])
		}
		wg.Wait()
		el := time.Since(start)
		must(firstErr)
		st := db.Stats()
		if st.OutOfOrder != 0 {
			must(fmt.Errorf("unexpected out-of-order samples: %d", st.OutOfOrder))
		}
		fmt.Printf("| %s | %d | %v | %.0f | %.2f |\n", mode, st.Samples, el.Round(time.Millisecond),
			float64(st.Samples)/el.Seconds(), st.BytesPerSample)
		if opts.Dir != "" {
			keep = db
		}
	}
	walBytes := dirSize(filepath.Join(dir, "wal"))
	st := keep.Stats()
	fmt.Printf("\nWAL on disk: %.1f MiB (%.2f bytes/sample). In-memory chunk data: %.1f MiB.\n\n",
		float64(walBytes)/(1<<20), float64(walBytes)/float64(st.Samples), float64(st.ChunkBytes)/(1<<20))
	return keep
}

func dirSize(dir string) int64 {
	var n int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n += info.Size()
		}
		return nil
	})
	return n
}

func queries(db *tsdb.DB, hosts, scrapes, iters int) {
	e := query.NewEngine(db)
	base := int64(1_700_000_000_000)
	end := base + int64(scrapes-1)*scrapeMs
	rng := rand.New(rand.NewSource(42))
	randomTime := func() int64 {
		// somewhere in the second half of the data, so 1h windows are full
		return end - rng.Int63n((end-base)/2)
	}
	type q struct {
		desc, expr string
		rangeQ     bool
	}
	host := fmt.Sprintf("host-%04d", hosts/2)
	qs := []q{
		{"single series, instant", fmt.Sprintf(`memory_bytes{host=%q}`, host), false},
		{"rate over 1 series, instant", fmt.Sprintf(`rate(http_requests_total{host=%q,code="200"}[5m])`, host), false},
		{fmt.Sprintf("sum by (code) of rate over %d series, instant", 2*hosts), `sum by (code) (rate(http_requests_total[5m]))`, false},
		{fmt.Sprintf("avg by (dc) over %d series, instant", hosts), `avg by (dc) (load1)`, false},
		{fmt.Sprintf("regex host match, max_over_time 10m, %d series", hosts/10), `max_over_time(temperature_celsius{host=~"host-...1"}[10m])`, false},
		{fmt.Sprintf("1h range, 1m step, sum of rate over %d series", hosts), `sum(rate(http_requests_total{code="200"}[5m]))`, true},
		{fmt.Sprintf("1h range, 1m step, %d output series", hosts), `cpu_seconds_total - 0`, true},
	}
	fmt.Printf("## Query latency (%d runs each at random times, single goroutine)\n\n", iters)
	fmt.Printf("| query | expression | p50 | p99 | result series |\n|---|---|---:|---:|---:|\n")
	for _, x := range qs {
		var lat []time.Duration
		nres := 0
		for i := 0; i < iters; i++ {
			ts := randomTime()
			start := time.Now()
			if x.rangeQ {
				m, err := e.Range(x.expr, ts-3600_000, ts, 60_000)
				must(err)
				nres = len(m)
			} else {
				v, err := e.Instant(x.expr, ts)
				must(err)
				nres = len(v.(query.Vector))
			}
			lat = append(lat, time.Since(start))
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		p := func(q float64) time.Duration { return lat[int(math.Ceil(q*float64(len(lat))))-1] }
		fmt.Printf("| %s | `%s` | %s | %s | %d |\n", x.desc, x.expr, fmtDur(p(0.5)), fmtDur(p(0.99)), nres)
	}
	fmt.Println()
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

func recovery(db *tsdb.DB, dir string) {
	want := db.Stats().Samples
	must(db.Close())
	fmt.Printf("## Recovery\n\n")
	fmt.Printf("| restart from | samples recovered | open time | samples/sec |\n|---|---:|---:|---:|\n")
	report := func(what string) *tsdb.DB {
		start := time.Now()
		db, err := tsdb.Open(tsdb.Options{Dir: dir})
		must(err)
		el := time.Since(start)
		got := db.Stats().Samples
		if got != want {
			must(fmt.Errorf("%s: recovered %d samples, want %d", what, got, want))
		}
		fmt.Printf("| %s | %d | %v | %.0f |\n", what, got, el.Round(time.Millisecond), float64(got)/el.Seconds())
		return db
	}
	db = report("WAL replay only")
	start := time.Now()
	cs, err := db.Checkpoint()
	must(err)
	ckptTime := time.Since(start)
	must(db.Close())
	db = report("checkpoint")
	must(db.Close())
	fmt.Printf("\nWriting the checkpoint took %v (%.1f MiB, %d chunks).\n",
		ckptTime.Round(time.Millisecond), float64(cs.Bytes)/(1<<20), cs.Chunks)
}
