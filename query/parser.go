package query

import (
	"fmt"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// ParseError reports a syntax or type error with its byte offset.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string { return fmt.Sprintf("parse error at %d: %s", e.Pos, e.Msg) }

var aggregateOps = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true}

type parser struct {
	toks []token
	i    int
}

// Parse parses and type-checks a query.
//
// Grammar (lowest to highest precedence):
//
//	expr    = term { ("+" | "-") term }
//	term    = unary { ("*" | "/") unary }
//	unary   = "-" unary | primary
//	primary = NUMBER | "(" expr ")" | aggregation | call | selector [ "[" DURATION "]" ]
func Parse(input string) (Expr, error) {
	toks, err := lex(input)
	if err != nil {
		return nil, &ParseError{Msg: err.Error()}
	}
	p := &parser{toks: toks}
	var e Expr
	err = p.catch(func() { e = p.expr() })
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, &ParseError{Pos: p.peek().pos, Msg: fmt.Sprintf("unexpected %s", p.peek().kind)}
	}
	return e, nil
}

// catch converts the parser's internal panics into errors.
func (p *parser) catch(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pe, ok := r.(*ParseError)
			if !ok {
				panic(r)
			}
			err = pe
		}
	}()
	fn()
	return nil
}

func (p *parser) fail(pos int, format string, args ...any) {
	panic(&ParseError{Pos: pos, Msg: fmt.Sprintf(format, args...)})
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tEOF {
		p.i++
	}
	return t
}

func (p *parser) expect(k tokenKind) token {
	t := p.next()
	if t.kind != k {
		p.fail(t.pos, "expected %s, got %s", k, t.kind)
	}
	return t
}

func (p *parser) expr() Expr {
	lhs := p.term()
	for k := p.peek().kind; k == tAdd || k == tSub; k = p.peek().kind {
		op := p.next()
		lhs = p.binary(op, lhs, p.term())
	}
	return lhs
}

func (p *parser) term() Expr {
	lhs := p.unary()
	for k := p.peek().kind; k == tMul || k == tDiv; k = p.peek().kind {
		op := p.next()
		lhs = p.binary(op, lhs, p.unary())
	}
	return lhs
}

func (p *parser) binary(op token, lhs, rhs Expr) Expr {
	for _, side := range []Expr{lhs, rhs} {
		if side.Type() == TypeMatrix {
			p.fail(op.pos, "binary %s needs scalar or instant vector operands, got range vector", op.text)
		}
	}
	if l, ok := lhs.(*NumberLiteral); ok {
		if r, ok := rhs.(*NumberLiteral); ok {
			return &NumberLiteral{Val: scalarOp(op.kind, l.Val, r.Val)}
		}
	}
	return &BinaryExpr{Op: op.kind, LHS: lhs, RHS: rhs}
}

func (p *parser) unary() Expr {
	if p.peek().kind == tSub {
		op := p.next()
		operand := p.unary()
		if n, ok := operand.(*NumberLiteral); ok {
			return &NumberLiteral{Val: -n.Val}
		}
		return p.binary(token{kind: tMul, text: "*", pos: op.pos}, &NumberLiteral{Val: -1}, operand)
	}
	if p.peek().kind == tAdd {
		p.next()
		return p.unary()
	}
	return p.primary()
}

func (p *parser) primary() Expr {
	t := p.peek()
	switch t.kind {
	case tNumber:
		p.next()
		return &NumberLiteral{Val: t.num}
	case tLParen:
		p.next()
		e := p.expr()
		p.expect(tRParen)
		return e
	case tLBrace:
		return p.maybeRange(p.selector(""))
	case tIdent:
		p.next()
		if aggregateOps[t.text] && (p.peek().kind == tLParen || isGroupingKeyword(p.peek())) {
			return p.aggregation(t)
		}
		if p.peek().kind == tLParen {
			return p.call(t)
		}
		return p.maybeRange(p.selector(t.text))
	}
	p.fail(t.pos, "unexpected %s", t.kind)
	return nil
}

func isGroupingKeyword(t token) bool {
	return t.kind == tIdent && (t.text == "by" || t.text == "without")
}

func (p *parser) maybeRange(vs *VectorSelector) Expr {
	if p.peek().kind != tLBracket {
		return vs
	}
	p.next()
	d := p.expect(tDuration)
	p.expect(tRBracket)
	return &MatrixSelector{VS: vs, Range: d.dur}
}

func (p *parser) selector(name string) *VectorSelector {
	vs := &VectorSelector{Name: name}
	pos := p.peek().pos
	if name != "" {
		vs.Matchers = append(vs.Matchers, labels.MustMatcher(labels.MatchEqual, labels.MetricName, name))
	}
	if p.peek().kind == tLBrace {
		p.next()
		for p.peek().kind != tRBrace {
			nameTok := p.expect(tIdent)
			opTok := p.next()
			var mt labels.MatchType
			switch opTok.kind {
			case tEq:
				mt = labels.MatchEqual
			case tNeq:
				mt = labels.MatchNotEqual
			case tEqRe:
				mt = labels.MatchRegexp
			case tNeqRe:
				mt = labels.MatchNotRegexp
			default:
				p.fail(opTok.pos, "expected label matcher operator, got %s", opTok.kind)
			}
			val := p.expect(tString)
			m, err := labels.NewMatcher(mt, nameTok.text, val.text)
			if err != nil {
				p.fail(val.pos, "%v", err)
			}
			vs.Matchers = append(vs.Matchers, m)
			if p.peek().kind == tComma {
				p.next()
				continue
			}
			if p.peek().kind != tRBrace {
				p.fail(p.peek().pos, "expected , or }, got %s", p.peek().kind)
			}
		}
		p.next()
	}
	// Like Prometheus, refuse selectors that would match every series.
	nonEmpty := false
	for _, m := range vs.Matchers {
		if !m.Matches("") {
			nonEmpty = true
		}
	}
	if !nonEmpty {
		p.fail(pos, "vector selector must contain at least one matcher that does not match the empty string")
	}
	return vs
}

func (p *parser) grouping() []string {
	p.expect(tLParen)
	var names []string
	for p.peek().kind != tRParen {
		names = append(names, p.expect(tIdent).text)
		if p.peek().kind == tComma {
			p.next()
		}
	}
	p.next()
	return names
}

func (p *parser) aggregation(op token) Expr {
	agg := &AggregateExpr{Op: op.text}
	parseGrouping := func() {
		kw := p.next()
		agg.Without = kw.text == "without"
		agg.Grouping = p.grouping()
	}
	if isGroupingKeyword(p.peek()) {
		parseGrouping()
	}
	p.expect(tLParen)
	agg.Expr = p.expr()
	p.expect(tRParen)
	if isGroupingKeyword(p.peek()) {
		if agg.Grouping != nil || agg.Without {
			p.fail(p.peek().pos, "duplicate grouping clause")
		}
		parseGrouping()
	}
	if agg.Expr.Type() != TypeVector {
		p.fail(op.pos, "%s expects an instant vector, got %s", op.text, agg.Expr.Type())
	}
	return agg
}

func (p *parser) call(name token) Expr {
	fn, ok := functions[name.text]
	if !ok {
		p.fail(name.pos, "unknown function %q", name.text)
	}
	p.expect(tLParen)
	var args []Expr
	for p.peek().kind != tRParen {
		args = append(args, p.expr())
		if p.peek().kind == tComma {
			p.next()
		} else if p.peek().kind != tRParen {
			p.fail(p.peek().pos, "expected , or ), got %s", p.peek().kind)
		}
	}
	p.next()
	if len(args) != len(fn.args) {
		p.fail(name.pos, "%s expects %d argument(s), got %d", fn.name, len(fn.args), len(args))
	}
	for i, a := range args {
		if a.Type() != fn.args[i] {
			p.fail(name.pos, "%s argument %d must be %s, got %s", fn.name, i+1, fn.args[i], a.Type())
		}
	}
	return &Call{Func: fn, Args: args}
}
