package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type tokenKind int

const (
	tEOF tokenKind = iota
	tIdent
	tNumber
	tString
	tDuration
	tLBrace
	tRBrace
	tLParen
	tRParen
	tLBracket
	tRBracket
	tComma
	tEq
	tNeq
	tEqRe
	tNeqRe
	tAdd
	tSub
	tMul
	tDiv
)

var tokenNames = map[tokenKind]string{
	tEOF: "end of input", tIdent: "identifier", tNumber: "number", tString: "string",
	tDuration: "duration", tLBrace: "{", tRBrace: "}", tLParen: "(", tRParen: ")",
	tLBracket: "[", tRBracket: "]", tComma: ",", tEq: "=", tNeq: "!=", tEqRe: "=~",
	tNeqRe: "!~", tAdd: "+", tSub: "-", tMul: "*", tDiv: "/",
}

func (k tokenKind) String() string { return tokenNames[k] }

type token struct {
	kind tokenKind
	text string
	pos  int
	num  float64       // tNumber
	dur  time.Duration // tDuration
}

func isIdentStart(c byte) bool {
	return c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// lex splits a query into tokens.
func lex(input string) ([]token, error) {
	var toks []token
	i := 0
	for {
		for i < len(input) && strings.IndexByte(" \t\r\n", input[i]) >= 0 {
			i++
		}
		if i >= len(input) {
			toks = append(toks, token{kind: tEOF, pos: i})
			return toks, nil
		}
		start := i
		c := input[i]
		two := ""
		if i+1 < len(input) {
			two = input[i : i+2]
		}
		switch {
		case two == "!=":
			toks = append(toks, token{kind: tNeq, text: two, pos: start})
			i += 2
		case two == "=~":
			toks = append(toks, token{kind: tEqRe, text: two, pos: start})
			i += 2
		case two == "!~":
			toks = append(toks, token{kind: tNeqRe, text: two, pos: start})
			i += 2
		case strings.IndexByte("{}()[],=+-*/", c) >= 0:
			kind := map[byte]tokenKind{'{': tLBrace, '}': tRBrace, '(': tLParen, ')': tRParen,
				'[': tLBracket, ']': tRBracket, ',': tComma, '=': tEq, '+': tAdd, '-': tSub,
				'*': tMul, '/': tDiv}[c]
			toks = append(toks, token{kind: kind, text: string(c), pos: start})
			i++
		case c == '"' || c == '\'':
			s, n, err := lexString(input[i:])
			if err != nil {
				return nil, fmt.Errorf("at %d: %w", start, err)
			}
			toks = append(toks, token{kind: tString, text: s, pos: start})
			i += n
		case isDigit(c) || (c == '.' && i+1 < len(input) && isDigit(input[i+1])):
			j := i
			for j < len(input) && (isDigit(input[j]) || input[j] == '.') {
				j++
			}
			// A digit run followed by a unit letter is a duration (5m, 1h30m).
			if j < len(input) && strings.IndexByte("smhdwy", input[j]) >= 0 {
				for j < len(input) && (isDigit(input[j]) || strings.IndexByte("smhdwy", input[j]) >= 0) {
					j++
				}
				d, err := parseDuration(input[i:j])
				if err != nil {
					return nil, fmt.Errorf("at %d: %w", start, err)
				}
				toks = append(toks, token{kind: tDuration, text: input[i:j], pos: start, dur: d})
				i = j
				continue
			}
			if j < len(input) && (input[j] == 'e' || input[j] == 'E') {
				j++
				if j < len(input) && (input[j] == '+' || input[j] == '-') {
					j++
				}
				for j < len(input) && isDigit(input[j]) {
					j++
				}
			}
			f, err := strconv.ParseFloat(input[i:j], 64)
			if err != nil {
				return nil, fmt.Errorf("at %d: bad number %q", start, input[i:j])
			}
			toks = append(toks, token{kind: tNumber, text: input[i:j], pos: start, num: f})
			i = j
		case isIdentStart(c):
			j := i
			for j < len(input) && isIdentChar(input[j]) {
				j++
			}
			toks = append(toks, token{kind: tIdent, text: input[i:j], pos: start})
			i = j
		default:
			return nil, fmt.Errorf("at %d: unexpected character %q", start, c)
		}
	}
}

func lexString(s string) (string, int, error) {
	quote := s[0]
	for j := 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			j++
		case quote:
			body := s[1:j]
			if quote == '\'' {
				body = strings.ReplaceAll(body, `\'`, `'`)
				body = strings.ReplaceAll(body, `"`, `\"`)
			}
			out, err := strconv.Unquote(`"` + body + `"`)
			if err != nil {
				return "", 0, fmt.Errorf("bad string literal %s", s[:j+1])
			}
			return out, j + 1, nil
		}
	}
	return "", 0, fmt.Errorf("unterminated string")
}

var unitDurations = map[string]time.Duration{
	"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour,
	"d": 24 * time.Hour, "w": 7 * 24 * time.Hour, "y": 365 * 24 * time.Hour,
}

// parseDuration parses Prometheus-style durations such as 30s, 5m or 1h30m.
func parseDuration(s string) (time.Duration, error) {
	var total time.Duration
	orig := s
	for s != "" {
		j := 0
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		if j == 0 {
			return 0, fmt.Errorf("bad duration %q", orig)
		}
		n, _ := strconv.Atoi(s[:j])
		s = s[j:]
		unit := ""
		if strings.HasPrefix(s, "ms") {
			unit = "ms"
		} else if s != "" {
			unit = s[:1]
		}
		d, ok := unitDurations[unit]
		if !ok {
			return 0, fmt.Errorf("bad duration %q", orig)
		}
		total += time.Duration(n) * d
		s = s[len(unit):]
	}
	if total <= 0 {
		return 0, fmt.Errorf("duration must be positive: %q", orig)
	}
	return total, nil
}
