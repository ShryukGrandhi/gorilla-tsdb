package query

import (
	"math"
	"strings"
	"testing"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

func TestParseValid(t *testing.T) {
	cases := map[string]string{
		`up`:                                  `up`,
		`up{job="api", env!='dev'}`:           `up{job="api",env!="dev"}`,
		`{__name__=~"http_.*"}`:               `{__name__=~"http_.*"}`,
		`rate(http_requests_total[5m])`:       `rate(http_requests_total[5m])`,
		`sum by (job) (rate(x[1h30m]))`:       `sum by (job) (rate(x[1h30m]))`,
		`sum(rate(x[1m])) without (instance)`: `sum without (instance) (rate(x[1m]))`,
		`1 + 2 * 3`:                           `7`,
		`-x`:                                  `(-1 * x)`,
		`a / b * 100`:                         `((a / b) * 100)`,
		`a - (b - c)`:                         `(a - (b - c))`,
		`max_over_time(cpu{host=~"web-[0-9]+"}[30s])`: `max_over_time(cpu{host=~"web-[0-9]+"}[30s])`,
		`1e3 + .5`: `1000.5`,
	}
	for in, want := range cases {
		e, err := Parse(in)
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got := e.String(); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		`rate(x)`:               "must be range vector",
		`sum(x[5m])`:            "expects an instant vector",
		`x[5m] + 1`:             "range vector",
		`{job!="a"}`:            "at least one matcher",
		`foo(x)`:                "unknown function",
		`up{job="a"`:            "expected",
		`up{job~"a"}`:           "unexpected character",
		`x[5]`:                  "expected duration",
		`rate(x[5m], 1)`:        "argument",
		`up{job=~"("}`:          "invalid regexp",
		`sum by (a) (x) by (b)`: "duplicate grouping",
		`"unterminated`:         "unterminated",
		`up )`:                  "unexpected )",
	}
	for in, want := range cases {
		_, err := Parse(in)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want error containing %q", in, err, want)
		}
	}
}

const sec = int64(1000)

// fixture: two counters increasing at 1/s and 2/s, scraped every 15s for
// 10 minutes, plus a gauge per host.
func fixture(t *testing.T) *tsdb.DB {
	t.Helper()
	db, err := tsdb.Open(tsdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	app := db.Appender()
	for ts := int64(0); ts <= 600; ts += 15 {
		for i, host := range []string{"a", "b"} {
			rate := float64(i + 1)
			_ = app.Add(labels.FromStrings("__name__", "req_total", "job", "api", "host", host), ts*sec, rate*float64(ts))
			_ = app.Add(labels.FromStrings("__name__", "temp", "job", "api", "host", host), ts*sec, 20+rate)
			_ = app.Add(labels.FromStrings("__name__", "limit", "job", "api", "host", host), ts*sec, 100)
		}
		_ = app.Add(labels.FromStrings("__name__", "temp", "job", "db", "host", "c"), ts*sec, 50)
	}
	if _, err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func instantVector(t *testing.T, e *Engine, q string, ts int64) map[string]float64 {
	t.Helper()
	v, err := e.Instant(q, ts)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	out := map[string]float64{}
	switch r := v.(type) {
	case Vector:
		for _, s := range r {
			out[s.Labels.String()] = s.V
		}
	case Scalar:
		out["scalar"] = r.V
	default:
		t.Fatalf("%s: unexpected %T", q, v)
	}
	return out
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func expect(t *testing.T, q string, got, want map[string]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v want %v", q, got, want)
	}
	for k, w := range want {
		if g, ok := got[k]; !ok || !approx(g, w) {
			t.Fatalf("%s: %s = %v (present=%v), want %v; full result %v", q, k, g, ok, w, got)
		}
	}
}

func TestInstantQueries(t *testing.T) {
	e := NewEngine(fixture(t))
	at := 300 * sec
	cases := []struct {
		q    string
		want map[string]float64
	}{
		{`req_total{host="a"}`, map[string]float64{`req_total{host="a", job="api"}`: 300}},
		{`rate(req_total[1m])`, map[string]float64{`{host="a", job="api"}`: 1, `{host="b", job="api"}`: 2}},
		{`increase(req_total{host="b"}[2m])`, map[string]float64{`{host="b", job="api"}`: 240}},
		{`irate(req_total[1m])`, map[string]float64{`{host="a", job="api"}`: 1, `{host="b", job="api"}`: 2}},
		{`sum(rate(req_total[1m]))`, map[string]float64{`{}`: 3}},
		{`sum by (job) (temp)`, map[string]float64{`{job="api"}`: 43, `{job="db"}`: 50}},
		{`avg without (host) (temp)`, map[string]float64{`{job="api"}`: 21.5, `{job="db"}`: 50}},
		{`count(temp)`, map[string]float64{`{}`: 3}},
		{`max(temp) - min(temp)`, map[string]float64{`{}`: 29}},
		{`temp{job="api"} / limit * 100`, map[string]float64{`{host="a", job="api"}`: 21, `{host="b", job="api"}`: 22}},
		{`-temp{host="c"}`, map[string]float64{`{host="c", job="db"}`: -50}},
		{`count_over_time(temp{host="c"}[1m])`, map[string]float64{`{host="c", job="db"}`: 4}},
		{`sum_over_time(req_total{host="a"}[30s])`, map[string]float64{`{host="a", job="api"}`: 285 + 300}},
		{`avg_over_time(req_total{host="a"}[1m])`, map[string]float64{`{host="a", job="api"}`: (255 + 270 + 285 + 300) / 4.0}},
		{`min_over_time(req_total{host="b"}[1m])`, map[string]float64{`{host="b", job="api"}`: 510}},
		{`max_over_time(req_total{host="b"}[1m])`, map[string]float64{`{host="b", job="api"}`: 600}},
		{`abs(-temp{host="c"})`, map[string]float64{`{host="c", job="db"}`: 50}},
		{`2 * 3 + 1`, map[string]float64{"scalar": 7}},
		{`temp{job="nope"}`, map[string]float64{}},
	}
	for _, c := range cases {
		expect(t, c.q, instantVector(t, e, c.q, at), c.want)
	}
}

func TestLookbackDelta(t *testing.T) {
	e := NewEngine(fixture(t))
	// Last sample is at 600s; 5m lookback means it is visible until 900s.
	if got := instantVector(t, e, `temp{host="c"}`, 899*sec); len(got) != 1 {
		t.Fatalf("expected sample within lookback, got %v", got)
	}
	if got := instantVector(t, e, `temp{host="c"}`, 900*sec); len(got) != 0 {
		t.Fatalf("expected stale series to vanish, got %v", got)
	}
}

func TestRateHandlesCounterReset(t *testing.T) {
	pts := []Point{{0, 0}, {10_000, 10}, {20_000, 20}, {30_000, 5}, {40_000, 15}}
	// raw increase = (15 - 0) + 20 (value lost at the reset) = 35 over 40s;
	// window [0, 40s] is fully covered, so no extrapolation.
	v, ok := extrapolatedDelta(pts, 0, 40_000, true, false)
	if !ok || !approx(v, 35) {
		t.Fatalf("increase = %v", v)
	}
	if v, _ := irate(pts[:4], 0, 0); !approx(v, 0.5) {
		t.Fatalf("irate after reset = %v", v)
	}
	if _, ok := extrapolatedDelta(pts[:1], 0, 10, true, true); ok {
		t.Fatal("rate of one sample must be empty")
	}
}

func TestRateDoesNotExtrapolateBelowZero(t *testing.T) {
	// Counter starts at 1 shortly after window start; extrapolating a full
	// half-interval back would imply a negative value.
	pts := []Point{{50_000, 1}, {60_000, 11}, {70_000, 21}}
	v, _ := extrapolatedDelta(pts, 0, 70_000, true, false)
	// durationToZero = 20s * 1/20 = 1s, so extrapolate 1s back: 20 * 21/20.
	if !approx(v, 21) {
		t.Fatalf("increase = %v", v)
	}
}

func TestRangeQuery(t *testing.T) {
	e := NewEngine(fixture(t))
	m, err := e.Range(`sum by (host) (rate(req_total[1m]))`, 120*sec, 600*sec, 60*sec)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("series = %d", len(m))
	}
	for i, s := range m {
		if len(s.Points) != 9 {
			t.Fatalf("points = %d", len(s.Points))
		}
		for _, p := range s.Points {
			if !approx(p.V, float64(i+1)) {
				t.Fatalf("%v at %d = %v", s.Labels, p.T, p.V)
			}
		}
	}
	sc, err := e.Range(`1 + 1`, 0, 10*sec, 5*sec)
	if err != nil || len(sc) != 1 || len(sc[0].Points) != 3 {
		t.Fatalf("scalar range: %v %v", sc, err)
	}
	if _, err := e.Range(`req_total[1m]`, 0, 1, 1); err == nil {
		t.Fatal("range vector result should be rejected")
	}
	if _, err := e.Range(`up`, 0, 1<<40, 1); err == nil {
		t.Fatal("step limit not enforced")
	}
}

func TestInstantMatrix(t *testing.T) {
	e := NewEngine(fixture(t))
	v, err := e.Instant(`req_total{host="a"}[45s]`, 300*sec)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(Matrix)
	if len(m) != 1 || len(m[0].Points) != 3 || m[0].Points[0].T != 270*sec {
		t.Fatalf("matrix = %+v", m)
	}
}
