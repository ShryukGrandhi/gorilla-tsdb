package lineproto

import (
	"strings"
	"testing"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

func TestParseLine(t *testing.T) {
	e, err := ParseLine(`http_requests_total{code="200", path="/a \"b\""} 1027 1700000000123`)
	if err != nil {
		t.Fatal(err)
	}
	want := labels.FromStrings("__name__", "http_requests_total", "code", "200", "path", `/a "b"`)
	if !labels.Equal(e.Labels, want) || e.Value != 1027 || !e.HasT || e.T != 1700000000123 {
		t.Fatalf("got %+v", e)
	}
	e, err = ParseLine(`up 1`)
	if err != nil || e.HasT || e.Value != 1 || e.Labels.Get("__name__") != "up" {
		t.Fatalf("got %+v %v", e, err)
	}
	e, err = ParseLine(`x{} -Inf`)
	if err != nil || e.Value > 0 {
		t.Fatalf("got %+v %v", e, err)
	}
}

func TestParseLineErrors(t *testing.T) {
	for _, in := range []string{
		`9bad 1`, `x{a=1} 1`, `x{a="1" 1`, `x{="1"} 1`, `x abc`, `x 1 2 3`, `x 1 1.5`, `x{a="1"}`,
	} {
		if _, err := ParseLine(in); err == nil {
			t.Errorf("%q: expected error", in)
		}
	}
}

func TestParseSkipsCommentsAndReportsLine(t *testing.T) {
	in := "# HELP x\n\nx 1\n  \ny{a=\"b\"} 2 5\nbad line here\n"
	var got []Entry
	err := Parse(strings.NewReader(in), func(e Entry) error { got = append(got, e); return nil })
	if err == nil || !strings.Contains(err.Error(), "line 6") {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("parsed %d entries", len(got))
	}
}
