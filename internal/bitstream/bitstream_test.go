package bitstream

import (
	"math/rand"
	"testing"
)

func TestRoundTripMixedWidths(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	type item struct {
		v uint64
		n int
	}
	var items []item
	w := NewWriter(0)
	total := 0
	for i := 0; i < 5000; i++ {
		n := rng.Intn(65)
		v := rng.Uint64()
		if n < 64 {
			v &= 1<<uint(n) - 1
		}
		if rng.Intn(4) == 0 {
			n = 1
			v &= 1
			w.WriteBit(v == 1)
		} else {
			w.WriteBits(v, n)
		}
		items = append(items, item{v, n})
		total += n
	}
	if w.BitLen() != total {
		t.Fatalf("BitLen = %d, want %d", w.BitLen(), total)
	}
	r := NewReader(w.Bytes())
	for i, it := range items {
		got, err := r.ReadBits(it.n)
		if err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		if got != it.v {
			t.Fatalf("item %d: got %x want %x (n=%d)", i, got, it.v, it.n)
		}
	}
}

func TestReadPastEnd(t *testing.T) {
	w := NewWriter(0)
	w.WriteBits(0b101, 3)
	r := NewReader(w.Bytes())
	if _, err := r.ReadBits(8); err != nil {
		t.Fatalf("reading padded byte should succeed: %v", err)
	}
	if _, err := r.ReadBit(); err != ErrEOF {
		t.Fatalf("expected ErrEOF, got %v", err)
	}
}
