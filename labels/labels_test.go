package labels

import "testing"

func TestNewSortsAndDedups(t *testing.T) {
	ls := New(Label{"b", "2"}, Label{"a", "1"}, Label{"c", ""}, Label{"a", "3"})
	want := Labels{{"a", "3"}, {"b", "2"}}
	if !Equal(ls, want) {
		t.Fatalf("got %v want %v", ls, want)
	}
}

func TestHashDistinguishesBoundaries(t *testing.T) {
	a := FromStrings("ab", "c")
	b := FromStrings("a", "bc")
	if a.Hash() == b.Hash() {
		t.Fatal("hash collision across name/value boundary")
	}
	if FromStrings("x", "1", "y", "2").Hash() != FromStrings("y", "2", "x", "1").Hash() {
		t.Fatal("hash depends on input order")
	}
}

func TestString(t *testing.T) {
	ls := FromStrings(MetricName, "http_requests_total", "code", "200", "job", "api")
	if got := ls.String(); got != `http_requests_total{code="200", job="api"}` {
		t.Fatalf("got %s", got)
	}
	if got := (Labels{}).String(); got != "{}" {
		t.Fatalf("got %s", got)
	}
}

func TestKeepDrop(t *testing.T) {
	ls := FromStrings("a", "1", "b", "2", "c", "3")
	if got := ls.Keep("c", "a"); !Equal(got, FromStrings("a", "1", "c", "3")) {
		t.Fatalf("Keep: %v", got)
	}
	if got := ls.Drop("b"); !Equal(got, FromStrings("a", "1", "c", "3")) {
		t.Fatalf("Drop: %v", got)
	}
}

func TestMatchers(t *testing.T) {
	ls := FromStrings("job", "api", "env", "prod")
	cases := []struct {
		m    *Matcher
		want bool
	}{
		{MustMatcher(MatchEqual, "job", "api"), true},
		{MustMatcher(MatchEqual, "job", "ap"), false},
		{MustMatcher(MatchNotEqual, "job", "web"), true},
		{MustMatcher(MatchRegexp, "job", "a.*"), true},
		{MustMatcher(MatchRegexp, "job", "p"), false}, // anchored
		{MustMatcher(MatchNotRegexp, "env", "dev|staging"), true},
		{MustMatcher(MatchEqual, "missing", ""), true},
		{MustMatcher(MatchRegexp, "missing", ".+"), false},
	}
	for _, c := range cases {
		if got := MatchesLabels(ls, c.m); got != c.want {
			t.Errorf("%s: got %v want %v", c.m, got, c.want)
		}
	}
	if _, err := NewMatcher(MatchRegexp, "a", "("); err == nil {
		t.Fatal("expected regexp error")
	}
}
