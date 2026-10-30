package index

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

func refs(xs ...int) []SeriesRef {
	out := make([]SeriesRef, len(xs))
	for i, x := range xs {
		out[i] = SeriesRef(x)
	}
	return out
}

func TestSetOps(t *testing.T) {
	a := refs(1, 3, 5, 7, 9)
	b := refs(2, 3, 4, 9, 10)
	if got := Intersect(a, b); !reflect.DeepEqual(got, refs(3, 9)) {
		t.Errorf("Intersect = %v", got)
	}
	if got := Subtract(a, b); !reflect.DeepEqual(got, refs(1, 5, 7)) {
		t.Errorf("Subtract = %v", got)
	}
	if got := Union(a, b, refs(0, 11)); !reflect.DeepEqual(got, refs(0, 1, 2, 3, 4, 5, 7, 9, 10, 11)) {
		t.Errorf("Union = %v", got)
	}
}

func buildIndex() (*Postings, map[SeriesRef]labels.Labels) {
	p := NewPostings()
	all := map[SeriesRef]labels.Labels{}
	ref := SeriesRef(1)
	for _, job := range []string{"api", "web", "db"} {
		for _, env := range []string{"prod", "dev"} {
			for i := 0; i < 3; i++ {
				ls := labels.FromStrings("__name__", "up", "job", job, "env", env, "inst", string(rune('a'+i)))
				if job == "db" && i == 2 {
					ls = ls.Drop("env") // series with a missing label
				}
				all[ref] = ls
				ref++
			}
		}
	}
	// insert in shuffled order to exercise out-of-order insertion
	keys := make([]SeriesRef, 0, len(all))
	for r := range all {
		keys = append(keys, r)
	}
	rand.New(rand.NewSource(3)).Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, r := range keys {
		p.Add(r, all[r])
	}
	return p, all
}

// bruteForce is the reference implementation the index must agree with.
func bruteForce(all map[SeriesRef]labels.Labels, ms ...*labels.Matcher) []SeriesRef {
	var out []SeriesRef
	for r, ls := range all {
		if labels.MatchesLabels(ls, ms...) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestSelectAgreesWithBruteForce(t *testing.T) {
	p, all := buildIndex()
	m := labels.MustMatcher
	cases := [][]*labels.Matcher{
		{m(labels.MatchEqual, "job", "api")},
		{m(labels.MatchEqual, "job", "api"), m(labels.MatchEqual, "env", "prod")},
		{m(labels.MatchNotEqual, "env", "prod")},
		{m(labels.MatchEqual, "env", "")},
		{m(labels.MatchRegexp, "job", "api|web"), m(labels.MatchNotRegexp, "inst", "a|b")},
		{m(labels.MatchRegexp, "env", ".*"), m(labels.MatchEqual, "job", "db")},
		{m(labels.MatchRegexp, "env", ".+")},
		{m(labels.MatchEqual, "job", "nope")},
		{m(labels.MatchEqual, "__name__", "up"), m(labels.MatchNotEqual, "job", "db"), m(labels.MatchNotEqual, "env", "dev")},
	}
	for i, ms := range cases {
		got := p.Select(ms...)
		want := bruteForce(all, ms...)
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("case %d %v: got %v want %v", i, ms, got, want)
		}
	}
}

func TestDelete(t *testing.T) {
	p, all := buildIndex()
	del := map[SeriesRef]labels.Labels{}
	for r, ls := range all {
		if ls.Get("job") == "web" {
			del[r] = ls
		}
	}
	p.Delete(del)
	if got := p.Select(labels.MustMatcher(labels.MatchEqual, "job", "web")); len(got) != 0 {
		t.Fatalf("deleted series still indexed: %v", got)
	}
	if vals := p.LabelValues("job"); !reflect.DeepEqual(vals, []string{"api", "db"}) {
		t.Fatalf("LabelValues = %v", vals)
	}
	if n := len(p.Select()); n != len(all)-len(del) {
		t.Fatalf("all postings = %d want %d", n, len(all)-len(del))
	}
}
