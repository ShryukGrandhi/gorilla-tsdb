// Package labels defines the label sets that identify a time series and the
// matchers used to select them.
package labels

import (
	"sort"
	"strconv"
	"strings"
)

// MetricName is the reserved label holding the metric name.
const MetricName = "__name__"

// Label is a single name/value pair.
type Label struct {
	Name, Value string
}

// Labels is a set of labels sorted by name. Empty values are not stored.
type Labels []Label

// FromStrings builds Labels from alternating name, value arguments.
func FromStrings(kv ...string) Labels {
	if len(kv)%2 != 0 {
		panic("labels.FromStrings: odd number of arguments")
	}
	ls := make(Labels, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		ls = append(ls, Label{kv[i], kv[i+1]})
	}
	return New(ls...)
}

// FromMap builds Labels from a map.
func FromMap(m map[string]string) Labels {
	ls := make(Labels, 0, len(m))
	for k, v := range m {
		ls = append(ls, Label{k, v})
	}
	return New(ls...)
}

// New sorts the given labels, drops empty values and keeps the last value for
// duplicate names.
func New(ls ...Label) Labels {
	out := make(Labels, 0, len(ls))
	for _, l := range ls {
		if l.Value != "" {
			out = append(out, l)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	// de-duplicate, last write wins
	dedup := out[:0]
	for i, l := range out {
		if i+1 < len(out) && out[i+1].Name == l.Name {
			continue
		}
		dedup = append(dedup, l)
	}
	return dedup
}

// Get returns the value for name or "".
func (ls Labels) Get(name string) string {
	i := sort.Search(len(ls), func(i int) bool { return ls[i].Name >= name })
	if i < len(ls) && ls[i].Name == name {
		return ls[i].Value
	}
	return ""
}

// Hash returns an FNV-1a hash of the label set.
func (ls Labels) Hash() uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, l := range ls {
		for i := 0; i < len(l.Name); i++ {
			h ^= uint64(l.Name[i])
			h *= prime
		}
		h ^= 0xff
		h *= prime
		for i := 0; i < len(l.Value); i++ {
			h ^= uint64(l.Value[i])
			h *= prime
		}
		h ^= 0xfe
		h *= prime
	}
	return h
}

// Equal reports whether two label sets are identical.
func Equal(a, b Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Compare orders label sets lexicographically.
func Compare(a, b Labels) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := strings.Compare(a[i].Name, b[i].Name); c != 0 {
			return c
		}
		if c := strings.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

// Copy returns a deep copy (the strings are shared, which is safe).
func (ls Labels) Copy() Labels {
	return append(Labels(nil), ls...)
}

// Keep returns only the labels whose names are listed.
func (ls Labels) Keep(names ...string) Labels {
	out := make(Labels, 0, len(names))
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

// Drop returns the labels whose names are not listed.
func (ls Labels) Drop(names ...string) Labels {
	out := make(Labels, 0, len(ls))
outer:
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				continue outer
			}
		}
		out = append(out, l)
	}
	return out
}

// String renders the set as name{a="b", c="d"}.
func (ls Labels) String() string {
	var b strings.Builder
	name := ls.Get(MetricName)
	b.WriteString(name)
	n := 0
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if n == 0 {
			b.WriteByte('{')
		} else {
			b.WriteString(", ")
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
		n++
	}
	if n > 0 {
		b.WriteByte('}')
	} else if name == "" {
		b.WriteString("{}")
	}
	return b.String()
}

// Map returns the labels as a map.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}
