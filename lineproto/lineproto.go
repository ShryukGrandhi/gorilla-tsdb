// Package lineproto parses the text ingest format, one sample per line:
//
//	metric_name{label="value",other="x"} 12.5 1700000000000
//
// The label block is optional, the timestamp (milliseconds) is optional, and
// lines starting with '#' or containing only whitespace are skipped. This is
// the sample line syntax of the Prometheus text exposition format.
package lineproto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// Entry is one parsed sample.
type Entry struct {
	Labels labels.Labels
	Value  float64
	T      int64
	HasT   bool
}

func isNameStart(c byte) bool {
	return c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isNameChar(c byte) bool { return isNameStart(c) || (c >= '0' && c <= '9') }

// ParseLine parses a single non-empty, non-comment line.
func ParseLine(line string) (Entry, error) {
	var e Entry
	s := strings.TrimSpace(line)
	i := 0
	for i < len(s) && isNameChar(s[i]) {
		i++
	}
	if i == 0 || !isNameStart(s[0]) {
		return e, fmt.Errorf("invalid metric name in %q", line)
	}
	ls := []labels.Label{{Name: labels.MetricName, Value: s[:i]}}
	s = s[i:]
	if strings.HasPrefix(s, "{") {
		s = s[1:]
		for {
			s = strings.TrimLeft(s, " ")
			if strings.HasPrefix(s, "}") {
				s = s[1:]
				break
			}
			j := 0
			for j < len(s) && isNameChar(s[j]) {
				j++
			}
			if j == 0 || !isNameStart(s[0]) {
				return e, fmt.Errorf("invalid label name in %q", line)
			}
			name := s[:j]
			s = strings.TrimLeft(s[j:], " ")
			if !strings.HasPrefix(s, "=") {
				return e, fmt.Errorf("expected = after label %q", name)
			}
			s = strings.TrimLeft(s[1:], " ")
			if !strings.HasPrefix(s, `"`) {
				return e, fmt.Errorf("expected quoted value for label %q", name)
			}
			end := 1
			for end < len(s) && s[end] != '"' {
				if s[end] == '\\' {
					end++
				}
				end++
			}
			if end >= len(s) {
				return e, fmt.Errorf("unterminated value for label %q", name)
			}
			val, err := strconv.Unquote(s[:end+1])
			if err != nil {
				return e, fmt.Errorf("bad value for label %q: %v", name, err)
			}
			ls = append(ls, labels.Label{Name: name, Value: val})
			s = strings.TrimLeft(s[end+1:], " ")
			if strings.HasPrefix(s, ",") {
				s = s[1:]
			} else if !strings.HasPrefix(s, "}") {
				return e, fmt.Errorf("expected , or } in %q", line)
			}
		}
	}
	fields := strings.Fields(s)
	if len(fields) < 1 || len(fields) > 2 {
		return e, fmt.Errorf("expected value and optional timestamp in %q", line)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return e, fmt.Errorf("bad value %q", fields[0])
	}
	e.Value = v
	if len(fields) == 2 {
		t, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return e, fmt.Errorf("bad timestamp %q", fields[1])
		}
		e.T, e.HasT = t, true
	}
	e.Labels = labels.New(ls...)
	return e, nil
}

// Parse reads every line from r and calls fn for each entry. Parsing stops at
// the first malformed line, whose 1-based number is included in the error.
func Parse(r io.Reader, fn func(Entry) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed[0] == '#' {
			continue
		}
		e, err := ParseLine(trimmed)
		if err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return sc.Err()
}
