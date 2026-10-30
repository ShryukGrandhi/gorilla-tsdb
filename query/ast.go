// Package query implements a PromQL subset: vector and range selectors,
// rate-style and *_over_time functions, sum/avg/min/max/count aggregation
// with by/without, and + - * / arithmetic between scalars and vectors.
package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// ValueType is the static type of an expression.
type ValueType int

// Expression result types.
const (
	TypeScalar ValueType = iota
	TypeVector
	TypeMatrix
)

func (t ValueType) String() string {
	return [...]string{"scalar", "instant vector", "range vector"}[t]
}

// Expr is a node of the query syntax tree.
type Expr interface {
	Type() ValueType
	String() string
}

// NumberLiteral is a constant.
type NumberLiteral struct{ Val float64 }

// VectorSelector selects the latest sample of each matching series.
type VectorSelector struct {
	Name     string
	Matchers []*labels.Matcher
}

// MatrixSelector selects all samples in a trailing window.
type MatrixSelector struct {
	VS    *VectorSelector
	Range time.Duration
}

// Call applies a built-in function.
type Call struct {
	Func *function
	Args []Expr
}

// AggregateExpr groups a vector and reduces each group.
type AggregateExpr struct {
	Op       string
	Expr     Expr
	Grouping []string
	Without  bool
}

// BinaryExpr is arithmetic between two operands.
type BinaryExpr struct {
	Op       tokenKind
	LHS, RHS Expr
}

func (*NumberLiteral) Type() ValueType  { return TypeScalar }
func (*VectorSelector) Type() ValueType { return TypeVector }
func (*MatrixSelector) Type() ValueType { return TypeMatrix }
func (c *Call) Type() ValueType         { return c.Func.ret }
func (*AggregateExpr) Type() ValueType  { return TypeVector }
func (b *BinaryExpr) Type() ValueType {
	if b.LHS.Type() == TypeScalar && b.RHS.Type() == TypeScalar {
		return TypeScalar
	}
	return TypeVector
}

func (n *NumberLiteral) String() string { return strconv.FormatFloat(n.Val, 'g', -1, 64) }

func (v *VectorSelector) String() string {
	var ms []string
	for _, m := range v.Matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual && m.Value == v.Name {
			continue
		}
		ms = append(ms, m.String())
	}
	if len(ms) == 0 {
		return v.Name
	}
	return v.Name + "{" + strings.Join(ms, ",") + "}"
}

func (m *MatrixSelector) String() string {
	return fmt.Sprintf("%s[%s]", m.VS, formatDuration(m.Range))
}

func (c *Call) String() string {
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = a.String()
	}
	return c.Func.name + "(" + strings.Join(args, ", ") + ")"
}

func (a *AggregateExpr) String() string {
	s := a.Op
	if len(a.Grouping) > 0 || a.Without {
		kw := "by"
		if a.Without {
			kw = "without"
		}
		s += " " + kw + " (" + strings.Join(a.Grouping, ", ") + ")"
	}
	return s + " (" + a.Expr.String() + ")"
}

func (b *BinaryExpr) String() string {
	return "(" + b.LHS.String() + " " + b.Op.String() + " " + b.RHS.String() + ")"
}

func formatDuration(d time.Duration) string {
	units := []struct {
		s string
		d time.Duration
	}{{"w", 7 * 24 * time.Hour}, {"d", 24 * time.Hour}, {"h", time.Hour}, {"m", time.Minute}, {"s", time.Second}, {"ms", time.Millisecond}}
	var b strings.Builder
	for _, u := range units {
		if d >= u.d {
			fmt.Fprintf(&b, "%d%s", d/u.d, u.s)
			d %= u.d
		}
	}
	return b.String()
}
