package query

import (
	"sort"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// Point is one timestamped value (milliseconds).
type Point struct {
	T int64
	V float64
}

// Sample is a point belonging to a labelled series.
type Sample struct {
	Labels labels.Labels
	Point
}

// Vector is a set of samples sharing one timestamp.
type Vector []Sample

// SeriesPoints is a labelled sequence of points.
type SeriesPoints struct {
	Labels labels.Labels
	Points []Point
}

// Matrix is a set of series.
type Matrix []SeriesPoints

// Scalar is a single number.
type Scalar Point

// Value is one of Scalar, Vector or Matrix.
type Value interface{ valueType() ValueType }

func (Scalar) valueType() ValueType { return TypeScalar }
func (Vector) valueType() ValueType { return TypeVector }
func (Matrix) valueType() ValueType { return TypeMatrix }

func sortVector(v Vector) {
	sort.Slice(v, func(i, j int) bool { return labels.Compare(v[i].Labels, v[j].Labels) < 0 })
}

func sortMatrix(m Matrix) {
	sort.Slice(m, func(i, j int) bool { return labels.Compare(m[i].Labels, m[j].Labels) < 0 })
}
