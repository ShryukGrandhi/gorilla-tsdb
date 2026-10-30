package query

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

// Queryable is the storage interface the engine reads from.
type Queryable interface {
	Select(mint, maxt int64, ms ...*labels.Matcher) []tsdb.Series
}

// Engine evaluates queries against a Queryable.
type Engine struct {
	q Queryable
	// Lookback is how far back an instant selector looks for the latest sample.
	Lookback time.Duration
	// MaxSteps bounds the number of steps in a range query.
	MaxSteps int
}

// NewEngine returns an engine with a 5 minute lookback.
func NewEngine(q Queryable) *Engine {
	return &Engine{q: q, Lookback: 5 * time.Minute, MaxSteps: 11000}
}

// Instant evaluates a query at time ts (milliseconds).
func (e *Engine) Instant(qs string, ts int64) (Value, error) {
	expr, err := Parse(qs)
	if err != nil {
		return nil, err
	}
	ev := e.newEvaluator(expr, ts, ts)
	switch v := ev.eval(expr, ts).(type) {
	case Scalar:
		return v, nil
	case Vector:
		sortVector(v)
		return v, nil
	case []window:
		m := make(Matrix, 0, len(v))
		for _, w := range v {
			m = append(m, SeriesPoints{Labels: w.lset, Points: append([]Point(nil), w.points...)})
		}
		sortMatrix(m)
		return m, nil
	}
	return nil, errors.New("unexpected result type")
}

// Range evaluates a query at every step from start to end inclusive.
func (e *Engine) Range(qs string, start, end, step int64) (Matrix, error) {
	if step <= 0 {
		return nil, errors.New("step must be positive")
	}
	if end < start {
		return nil, errors.New("end before start")
	}
	if n := (end-start)/step + 1; n > int64(e.MaxSteps) {
		return nil, fmt.Errorf("range query would need %d steps; limit is %d", n, e.MaxSteps)
	}
	expr, err := Parse(qs)
	if err != nil {
		return nil, err
	}
	if expr.Type() == TypeMatrix {
		return nil, errors.New("range query must return a scalar or instant vector")
	}
	ev := e.newEvaluator(expr, start, end)
	byKey := map[string]int{}
	var out Matrix
	add := func(ls labels.Labels, p Point) {
		k := ls.String()
		i, ok := byKey[k]
		if !ok {
			i = len(out)
			byKey[k] = i
			out = append(out, SeriesPoints{Labels: ls})
		}
		out[i].Points = append(out[i].Points, p)
	}
	for ts := start; ts <= end; ts += step {
		switch v := ev.eval(expr, ts).(type) {
		case Scalar:
			add(nil, Point(v))
		case Vector:
			for _, s := range v {
				add(s.Labels, s.Point)
			}
		}
	}
	sortMatrix(out)
	return out, nil
}

// evaluator holds the series for every selector in the query, fetched once
// for the whole evaluation interval; each step then only binary-searches
// in-memory slices.
type evaluator struct {
	lookback int64
	data     map[*VectorSelector][]tsdb.Series
}

func (e *Engine) newEvaluator(expr Expr, start, end int64) *evaluator {
	ev := &evaluator{lookback: e.Lookback.Milliseconds(), data: map[*VectorSelector][]tsdb.Series{}}
	var walk func(Expr)
	walk = func(x Expr) {
		switch n := x.(type) {
		case *VectorSelector:
			ev.data[n] = e.q.Select(start-ev.lookback+1, end, n.Matchers...)
		case *MatrixSelector:
			ev.data[n.VS] = e.q.Select(start-n.Range.Milliseconds()+1, end, n.VS.Matchers...)
		case *Call:
			for _, a := range n.Args {
				walk(a)
			}
		case *AggregateExpr:
			walk(n.Expr)
		case *BinaryExpr:
			walk(n.LHS)
			walk(n.RHS)
		}
	}
	walk(expr)
	return ev
}

// samplesIn returns the samples with mint < t <= maxt.
func samplesIn(s []tsdb.Sample, mint, maxt int64) []tsdb.Sample {
	lo := sort.Search(len(s), func(i int) bool { return s[i].T > mint })
	hi := sort.Search(len(s), func(i int) bool { return s[i].T > maxt })
	return s[lo:hi]
}

func (ev *evaluator) eval(expr Expr, ts int64) any {
	switch n := expr.(type) {
	case *NumberLiteral:
		return Scalar{T: ts, V: n.Val}
	case *VectorSelector:
		var out Vector
		for _, s := range ev.data[n] {
			in := samplesIn(s.Samples, ts-ev.lookback, ts)
			if len(in) == 0 {
				continue
			}
			last := in[len(in)-1]
			out = append(out, Sample{Labels: s.Labels, Point: Point{T: ts, V: last.V}})
		}
		return out
	case *MatrixSelector:
		var out []window
		for _, s := range ev.data[n.VS] {
			in := samplesIn(s.Samples, ts-n.Range.Milliseconds(), ts)
			if len(in) == 0 {
				continue
			}
			pts := make([]Point, len(in))
			for i, x := range in {
				pts[i] = Point{T: x.T, V: x.V}
			}
			out = append(out, window{lset: s.Labels, points: pts})
		}
		return out
	case *Call:
		return ev.call(n, ts)
	case *AggregateExpr:
		return aggregate(n, ev.eval(n.Expr, ts).(Vector), ts)
	case *BinaryExpr:
		return binaryOp(n.Op, ev.eval(n.LHS, ts), ev.eval(n.RHS, ts), ts)
	}
	panic(fmt.Sprintf("unhandled expression %T", expr))
}

func (ev *evaluator) call(c *Call, ts int64) Vector {
	var out Vector
	if c.Func.overRange != nil {
		ms := c.Args[0].(*MatrixSelector)
		start := ts - ms.Range.Milliseconds()
		for _, w := range ev.eval(ms, ts).([]window) {
			if v, ok := c.Func.overRange(w.points, start, ts); ok {
				out = append(out, Sample{Labels: w.lset.Drop(labels.MetricName), Point: Point{T: ts, V: v}})
			}
		}
		return out
	}
	for _, s := range ev.eval(c.Args[0], ts).(Vector) {
		out = append(out, Sample{Labels: s.Labels.Drop(labels.MetricName), Point: Point{T: ts, V: c.Func.perSample(s.V)}})
	}
	return out
}

func groupLabels(a *AggregateExpr, ls labels.Labels) labels.Labels {
	if a.Without {
		return ls.Drop(append([]string{labels.MetricName}, a.Grouping...)...)
	}
	return ls.Keep(a.Grouping...)
}

func aggregate(a *AggregateExpr, in Vector, ts int64) Vector {
	type group struct {
		lset  labels.Labels
		value float64
		count int
		mean  float64
	}
	groups := map[string]*group{}
	var order []string
	for _, s := range in {
		gl := groupLabels(a, s.Labels)
		key := gl.String()
		g, ok := groups[key]
		if !ok {
			g = &group{lset: gl, value: s.V}
			groups[key] = g
			order = append(order, key)
		} else {
			switch a.Op {
			case "sum":
				g.value += s.V
			case "min":
				if s.V < g.value || math.IsNaN(g.value) {
					g.value = s.V
				}
			case "max":
				if s.V > g.value || math.IsNaN(g.value) {
					g.value = s.V
				}
			}
		}
		g.count++
		g.mean += (s.V - g.mean) / float64(g.count)
	}
	out := make(Vector, 0, len(groups))
	for _, k := range order {
		g := groups[k]
		v := g.value
		switch a.Op {
		case "avg":
			v = g.mean
		case "count":
			v = float64(g.count)
		}
		out = append(out, Sample{Labels: g.lset, Point: Point{T: ts, V: v}})
	}
	return out
}

func scalarOp(op tokenKind, a, b float64) float64 {
	switch op {
	case tAdd:
		return a + b
	case tSub:
		return a - b
	case tMul:
		return a * b
	case tDiv:
		return a / b
	}
	panic("unknown operator")
}

// binaryOp applies arithmetic. Vector/vector operations match series
// one-to-one on their full label sets minus the metric name; unmatched series
// are dropped. Arithmetic results never keep the metric name.
func binaryOp(op tokenKind, lhs, rhs any, ts int64) any {
	switch l := lhs.(type) {
	case Scalar:
		switch r := rhs.(type) {
		case Scalar:
			return Scalar{T: ts, V: scalarOp(op, l.V, r.V)}
		case Vector:
			out := make(Vector, 0, len(r))
			for _, s := range r {
				out = append(out, Sample{Labels: s.Labels.Drop(labels.MetricName), Point: Point{T: ts, V: scalarOp(op, l.V, s.V)}})
			}
			return out
		}
	case Vector:
		switch r := rhs.(type) {
		case Scalar:
			out := make(Vector, 0, len(l))
			for _, s := range l {
				out = append(out, Sample{Labels: s.Labels.Drop(labels.MetricName), Point: Point{T: ts, V: scalarOp(op, s.V, r.V)}})
			}
			return out
		case Vector:
			right := make(map[string]Sample, len(r))
			for _, s := range r {
				right[s.Labels.Drop(labels.MetricName).String()] = s
			}
			var out Vector
			for _, s := range l {
				ls := s.Labels.Drop(labels.MetricName)
				if m, ok := right[ls.String()]; ok {
					out = append(out, Sample{Labels: ls, Point: Point{T: ts, V: scalarOp(op, s.V, m.V)}})
				}
			}
			return out
		}
	}
	panic(fmt.Sprintf("bad operand types %T, %T", lhs, rhs))
}
