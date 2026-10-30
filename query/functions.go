package query

import (
	"math"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// window is the samples of one series inside a range selector's window.
type window struct {
	lset   labels.Labels
	points []Point
}

type function struct {
	name string
	args []ValueType
	ret  ValueType
	// exactly one of these is set
	overRange func(points []Point, rangeStart, rangeEnd int64) (float64, bool)
	perSample func(v float64) float64
}

var functions = map[string]*function{}

func registerRange(name string, fn func([]Point, int64, int64) (float64, bool)) {
	functions[name] = &function{name: name, args: []ValueType{TypeMatrix}, ret: TypeVector, overRange: fn}
}

func registerSample(name string, fn func(float64) float64) {
	functions[name] = &function{name: name, args: []ValueType{TypeVector}, ret: TypeVector, perSample: fn}
}

func init() {
	registerRange("rate", func(p []Point, s, e int64) (float64, bool) { return extrapolatedDelta(p, s, e, true, true) })
	registerRange("increase", func(p []Point, s, e int64) (float64, bool) { return extrapolatedDelta(p, s, e, true, false) })
	registerRange("delta", func(p []Point, s, e int64) (float64, bool) { return extrapolatedDelta(p, s, e, false, false) })
	registerRange("irate", irate)
	registerRange("avg_over_time", func(p []Point, _, _ int64) (float64, bool) {
		if len(p) == 0 {
			return 0, false
		}
		// incremental mean is more stable than sum/count for large values
		mean := 0.0
		for i, pt := range p {
			mean += (pt.V - mean) / float64(i+1)
		}
		return mean, true
	})
	registerRange("sum_over_time", func(p []Point, _, _ int64) (float64, bool) {
		if len(p) == 0 {
			return 0, false
		}
		var s float64
		for _, pt := range p {
			s += pt.V
		}
		return s, true
	})
	registerRange("min_over_time", func(p []Point, _, _ int64) (float64, bool) {
		if len(p) == 0 {
			return 0, false
		}
		m := p[0].V
		for _, pt := range p[1:] {
			if pt.V < m || math.IsNaN(m) {
				m = pt.V
			}
		}
		return m, true
	})
	registerRange("max_over_time", func(p []Point, _, _ int64) (float64, bool) {
		if len(p) == 0 {
			return 0, false
		}
		m := p[0].V
		for _, pt := range p[1:] {
			if pt.V > m || math.IsNaN(m) {
				m = pt.V
			}
		}
		return m, true
	})
	registerRange("count_over_time", func(p []Point, _, _ int64) (float64, bool) {
		return float64(len(p)), len(p) > 0
	})
	registerSample("abs", math.Abs)
	registerSample("ceil", math.Ceil)
	registerSample("floor", math.Floor)
	registerSample("sqrt", math.Sqrt)
}

// extrapolatedDelta follows Prometheus' rate/increase/delta semantics: the
// observed change (with counter resets compensated) is extrapolated towards
// the window edges, but by no more than half a sample interval unless the
// series really starts or ends near the edge. Counters are not extrapolated
// below zero.
func extrapolatedDelta(p []Point, rangeStart, rangeEnd int64, isCounter, isRate bool) (float64, bool) {
	if len(p) < 2 {
		return 0, false
	}
	first, last := p[0], p[len(p)-1]
	result := last.V - first.V
	if isCounter {
		prev := first.V
		for _, pt := range p[1:] {
			if pt.V < prev {
				result += prev
			}
			prev = pt.V
		}
	}
	durationToStart := float64(first.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-last.T) / 1000
	sampled := float64(last.T-first.T) / 1000
	avgStep := sampled / float64(len(p)-1)

	if isCounter && result > 0 && first.V >= 0 {
		durationToZero := sampled * (first.V / result)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}
	threshold := avgStep * 1.1
	extrapolateTo := sampled
	if durationToStart < threshold {
		extrapolateTo += durationToStart
	} else {
		extrapolateTo += avgStep / 2
	}
	if durationToEnd < threshold {
		extrapolateTo += durationToEnd
	} else {
		extrapolateTo += avgStep / 2
	}
	result *= extrapolateTo / sampled
	if isRate {
		result /= float64(rangeEnd-rangeStart) / 1000
	}
	return result, true
}

// irate is the per-second rate between the last two samples in the window.
func irate(p []Point, _, _ int64) (float64, bool) {
	if len(p) < 2 {
		return 0, false
	}
	a, b := p[len(p)-2], p[len(p)-1]
	dt := float64(b.T-a.T) / 1000
	if dt == 0 {
		return 0, false
	}
	d := b.V - a.V
	if b.V < a.V { // counter reset
		d = b.V
	}
	return d / dt, true
}
