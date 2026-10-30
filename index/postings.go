// Package index implements an in-memory inverted index from label pairs to
// sorted lists of series references ("postings"), and the set operations used
// to resolve label matchers.
package index

import (
	"sort"
	"sync"

	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// SeriesRef identifies a series inside one database.
type SeriesRef uint64

// Postings maps label name -> label value -> sorted series refs.
type Postings struct {
	mu  sync.RWMutex
	m   map[string]map[string][]SeriesRef
	all []SeriesRef
}

// NewPostings returns an empty index.
func NewPostings() *Postings {
	return &Postings{m: make(map[string]map[string][]SeriesRef)}
}

func insertSorted(list []SeriesRef, ref SeriesRef) []SeriesRef {
	if n := len(list); n == 0 || list[n-1] < ref {
		return append(list, ref)
	}
	i := sort.Search(len(list), func(i int) bool { return list[i] >= ref })
	if i < len(list) && list[i] == ref {
		return list
	}
	list = append(list, 0)
	copy(list[i+1:], list[i:])
	list[i] = ref
	return list
}

func removeSorted(list []SeriesRef, del map[SeriesRef]struct{}) []SeriesRef {
	out := list[:0]
	for _, r := range list {
		if _, ok := del[r]; !ok {
			out = append(out, r)
		}
	}
	return out
}

// Add indexes a series under each of its labels.
func (p *Postings) Add(ref SeriesRef, ls labels.Labels) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range ls {
		vals, ok := p.m[l.Name]
		if !ok {
			vals = make(map[string][]SeriesRef)
			p.m[l.Name] = vals
		}
		vals[l.Value] = insertSorted(vals[l.Value], ref)
	}
	p.all = insertSorted(p.all, ref)
}

// Delete removes the given series. lsets supplies their labels so only the
// affected lists are touched.
func (p *Postings) Delete(refs map[SeriesRef]labels.Labels) {
	if len(refs) == 0 {
		return
	}
	del := make(map[SeriesRef]struct{}, len(refs))
	for r := range refs {
		del[r] = struct{}{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	touched := map[labels.Label]struct{}{}
	for _, ls := range refs {
		for _, l := range ls {
			touched[l] = struct{}{}
		}
	}
	for l := range touched {
		vals := p.m[l.Name]
		if vals == nil {
			continue
		}
		list := removeSorted(vals[l.Value], del)
		if len(list) == 0 {
			delete(vals, l.Value)
			if len(vals) == 0 {
				delete(p.m, l.Name)
			}
		} else {
			vals[l.Value] = list
		}
	}
	p.all = removeSorted(p.all, del)
}

// LabelNames returns all label names, sorted.
func (p *Postings) LabelNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.m))
	for n := range p.m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// LabelValues returns all values for a label name, sorted.
func (p *Postings) LabelValues(name string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.m[name]))
	for v := range p.m[name] {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Select returns the sorted refs of series matching all matchers.
//
// Matchers that reject the empty string (e.g. job="api", job=~"a.+") are
// resolved to the union of postings for the values they accept and then
// intersected. Matchers that accept the empty string (e.g. job!="api",
// job=~".*") also match series lacking the label, so they are applied by
// subtracting the postings of the values they reject. If no positive matcher
// exists, the subtraction starts from the set of all series.
func (p *Postings) Select(ms ...*labels.Matcher) []SeriesRef {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var (
		result   []SeriesRef
		haveBase bool
		subtract [][]SeriesRef
	)
	for _, m := range ms {
		vals := p.m[m.Name]
		if m.Matches("") {
			var reject [][]SeriesRef
			for v, list := range vals {
				if !m.Matches(v) {
					reject = append(reject, list)
				}
			}
			if len(reject) > 0 {
				subtract = append(subtract, Union(reject...))
			}
			continue
		}
		var list []SeriesRef
		if m.Type == labels.MatchEqual {
			list = vals[m.Value]
		} else {
			var accept [][]SeriesRef
			for v, l := range vals {
				if m.Matches(v) {
					accept = append(accept, l)
				}
			}
			list = Union(accept...)
		}
		if !haveBase {
			result = append([]SeriesRef(nil), list...)
			haveBase = true
		} else {
			result = Intersect(result, list)
		}
		if len(result) == 0 {
			return nil
		}
	}
	if !haveBase {
		result = append([]SeriesRef(nil), p.all...)
	}
	for _, s := range subtract {
		result = Subtract(result, s)
	}
	return result
}

// Intersect returns refs present in both sorted lists.
func Intersect(a, b []SeriesRef) []SeriesRef {
	out := make([]SeriesRef, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// Subtract returns refs in a that are not in b. Both must be sorted.
func Subtract(a, b []SeriesRef) []SeriesRef {
	out := make([]SeriesRef, 0, len(a))
	j := 0
	for _, r := range a {
		for j < len(b) && b[j] < r {
			j++
		}
		if j < len(b) && b[j] == r {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Union merges any number of sorted lists into one sorted, de-duplicated list.
func Union(lists ...[]SeriesRef) []SeriesRef {
	switch len(lists) {
	case 0:
		return nil
	case 1:
		return append([]SeriesRef(nil), lists[0]...)
	}
	for len(lists) > 1 {
		next := make([][]SeriesRef, 0, (len(lists)+1)/2)
		for i := 0; i < len(lists); i += 2 {
			if i+1 == len(lists) {
				next = append(next, lists[i])
				continue
			}
			next = append(next, union2(lists[i], lists[i+1]))
		}
		lists = next
	}
	return lists[0]
}

func union2(a, b []SeriesRef) []SeriesRef {
	out := make([]SeriesRef, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}
