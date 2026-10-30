package chunk

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func roundTrip(t *testing.T, in []sample) *XOR {
	t.Helper()
	c := NewXOR()
	for i, s := range in {
		if err := c.Append(s.t, s.v); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	check := func(c *XOR) {
		it := c.Iterator()
		i := 0
		for it.Next() {
			ts, v := it.At()
			if ts != in[i].t {
				t.Fatalf("sample %d: t=%d want %d", i, ts, in[i].t)
			}
			if math.Float64bits(v) != math.Float64bits(in[i].v) {
				t.Fatalf("sample %d: v=%v want %v", i, v, in[i].v)
			}
			i++
		}
		if it.Err() != nil {
			t.Fatalf("iterator error: %v", it.Err())
		}
		if i != len(in) {
			t.Fatalf("decoded %d samples, want %d", i, len(in))
		}
	}
	check(c)
	ro, err := LoadXOR(append([]byte(nil), c.Bytes()...), c.NumSamples())
	if err != nil {
		t.Fatalf("LoadXOR: %v", err)
	}
	if len(in) > 0 && (ro.MinT() != in[0].t || ro.MaxT() != in[len(in)-1].t) {
		t.Fatalf("loaded bounds [%d,%d]", ro.MinT(), ro.MaxT())
	}
	check(ro)
	return c
}

func TestRoundTripRegular(t *testing.T) {
	var in []sample
	for i := 0; i < 120; i++ {
		in = append(in, sample{int64(1_700_000_000_000 + i*15_000), float64(i % 7)})
	}
	roundTrip(t, in)
}

func TestRoundTripRandomized(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		ts := rng.Int63n(1 << 50)
		v := rng.NormFloat64() * 1000
		var in []sample
		n := 1 + rng.Intn(300)
		for i := 0; i < n; i++ {
			switch rng.Intn(6) {
			case 0:
				ts += 1 + rng.Int63n(1<<40) // huge jumps exercise the 64-bit dod path
			case 1:
				ts += 1 + rng.Int63n(5)
			default:
				ts += 15_000 + rng.Int63n(200) - 100
			}
			switch rng.Intn(8) {
			case 0:
				v = math.Inf(1)
			case 1:
				v = math.NaN()
			case 2:
				v = -v
			case 3:
				v = math.Float64frombits(rng.Uint64())
			case 4:
				// unchanged
			default:
				v += rng.NormFloat64()
			}
			in = append(in, sample{ts, v})
		}
		roundTrip(t, in)
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	c := NewXOR()
	_ = c.Append(100, 1)
	if err := c.Append(100, 2); err != ErrOutOfOrder {
		t.Fatalf("duplicate timestamp: got %v", err)
	}
	if err := c.Append(50, 2); err != ErrOutOfOrder {
		t.Fatalf("older timestamp: got %v", err)
	}
	if c.NumSamples() != 1 {
		t.Fatalf("NumSamples = %d", c.NumSamples())
	}
}

func TestReadOnlyAppend(t *testing.T) {
	c, _ := LoadXOR(nil, 0)
	if err := c.Append(1, 1); err != ErrReadOnly {
		t.Fatalf("got %v", err)
	}
}

func TestConstantSeriesCompressesToAboutTwoBitsPerSample(t *testing.T) {
	c := NewXOR()
	for i := 0; i < 1000; i++ {
		_ = c.Append(int64(i*10_000), 42)
	}
	// 16 header bytes + first delta + 999 * (1 dod bit + 1 value bit).
	if c.Size() > 16+4+(999*2)/8+1 {
		t.Fatalf("constant series too large: %d bytes", c.Size())
	}
}

func TestIteratorSnapshotsCount(t *testing.T) {
	c := NewXOR()
	_ = c.Append(1, 1)
	it := c.Iterator()
	_ = c.Append(2, 2)
	n := 0
	for it.Next() {
		n++
	}
	if n != 1 {
		t.Fatalf("iterator saw %d samples, want 1", n)
	}
}

func BenchmarkAppend(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	v := 100.0
	b.ReportAllocs()
	c := NewXOR()
	for i := 0; i < b.N; i++ {
		if c.NumSamples() == 120 {
			c = NewXOR()
		}
		v += math.Round(rng.NormFloat64()*100) / 100
		_ = c.Append(int64(i)*15_000, v)
	}
}

func BenchmarkIterate(b *testing.B) {
	c := NewXOR()
	for i := 0; i < 120; i++ {
		_ = c.Append(int64(i)*15_000, float64(i)*1.5)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := c.Iterator()
		for it.Next() {
		}
	}
}

func TestDoDBucketBoundaries(t *testing.T) {
	// Exercise each bucket edge in both directions.
	var in []sample
	ts, delta := int64(1_000_000_000), int64(15_000)
	for _, dod := range []int64{0, 63, -64, 64, -65, 8191, -8192, 8192, -8193, 524287, -524288, 524288, -524289, 1 << 40} {
		delta += dod
		ts += delta
		in = append(in, sample{ts, float64(dod)})
	}
	roundTrip(t, in)
}
