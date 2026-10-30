// Package chunk implements the Gorilla time-series compression scheme
// (Pelkonen et al., VLDB 2015): delta-of-delta encoding for timestamps and
// XOR encoding for float64 values, packed into a bit stream.
//
// Layout of a chunk (bit stream):
//
//	t0                  64 bits, two's complement
//	v0                  64 bits, IEEE-754
//	for each later sample:
//	  delta-of-delta     '0'                         dod == 0
//	                     '10'   + 14 bits            dod in [-8192, 8191]
//	                     '110'  + 17 bits            dod in [-65536, 65535]
//	                     '1110' + 20 bits            dod in [-524288, 524287]
//	                     '1111' + 64 bits            anything else
//	  value XOR          '0'                         same value as before
//	                     '10' + meaningful bits      fits previous window
//	                     '11' + 5b leading + 6b len + meaningful bits
//
// The sample count is kept outside the bit stream so a reader knows where to
// stop without a terminator.
package chunk

import (
	"errors"
	"math"
	"math/bits"

	"github.com/ShryukGrandhi/gorilla-tsdb/internal/bitstream"
)

// ErrOutOfOrder is returned when a sample's timestamp is not strictly greater
// than the previous one.
var ErrOutOfOrder = errors.New("chunk: out-of-order sample")

// ErrReadOnly is returned when appending to a chunk loaded from bytes.
var ErrReadOnly = errors.New("chunk: read-only chunk")

// XOR is an appendable Gorilla-compressed chunk. It is not safe for
// concurrent use; callers synchronise externally.
type XOR struct {
	w        *bitstream.Writer
	ro       []byte // set for read-only chunks loaded from bytes
	num      int
	minT     int64
	maxT     int64
	readOnly bool

	// append state
	tDelta   int64
	lastV    float64
	leading  uint8
	trailing uint8
}

// NewXOR returns an empty chunk.
func NewXOR() *XOR {
	return &XOR{w: bitstream.NewWriter(128), leading: 0xff}
}

// LoadXOR wraps an encoded stream holding num samples. The chunk is read-only.
func LoadXOR(b []byte, num int) (*XOR, error) {
	c := &XOR{ro: b, num: num, readOnly: true}
	if num == 0 {
		return c, nil
	}
	it := c.Iterator()
	first := true
	for it.Next() {
		if first {
			c.minT = it.T()
			first = false
		}
		c.maxT = it.T()
	}
	if it.Err() != nil {
		return nil, it.Err()
	}
	return c, nil
}

// ReadOnly reports whether the chunk was loaded from bytes and cannot grow.
func (c *XOR) ReadOnly() bool { return c.readOnly }

// NumSamples returns the number of samples in the chunk.
func (c *XOR) NumSamples() int { return c.num }

// MinT returns the first timestamp (undefined for an empty chunk).
func (c *XOR) MinT() int64 { return c.minT }

// MaxT returns the last timestamp (undefined for an empty chunk).
func (c *XOR) MaxT() int64 { return c.maxT }

// Bytes returns the encoded bit stream. It aliases internal storage.
func (c *XOR) Bytes() []byte {
	if c.readOnly {
		return c.ro
	}
	return c.w.Bytes()
}

// Size returns the encoded size in bytes.
func (c *XOR) Size() int { return len(c.Bytes()) }

// Append adds a sample. Timestamps must be strictly increasing.
func (c *XOR) Append(t int64, v float64) error {
	if c.readOnly {
		return ErrReadOnly
	}
	switch c.num {
	case 0:
		c.w.WriteBits(uint64(t), 64)
		c.w.WriteBits(math.Float64bits(v), 64)
		c.minT = t
	default:
		if t <= c.maxT {
			return ErrOutOfOrder
		}
		delta := t - c.maxT
		dod := delta - c.tDelta
		writeDoD(c.w, dod)
		c.tDelta = delta
		c.writeValue(v)
	}
	if c.num == 0 {
		c.tDelta = 0
	}
	c.maxT = t
	c.lastV = v
	c.num++
	return nil
}

func fits(x int64, nbits uint) bool {
	return x >= -(1<<(nbits-1)) && x < 1<<(nbits-1)
}

func writeDoD(w *bitstream.Writer, dod int64) {
	switch {
	case dod == 0:
		w.WriteBit(false)
	case fits(dod, 14):
		w.WriteBits(0b10, 2)
		w.WriteBits(uint64(dod), 14)
	case fits(dod, 17):
		w.WriteBits(0b110, 3)
		w.WriteBits(uint64(dod), 17)
	case fits(dod, 20):
		w.WriteBits(0b1110, 4)
		w.WriteBits(uint64(dod), 20)
	default:
		w.WriteBits(0b1111, 4)
		w.WriteBits(uint64(dod), 64)
	}
}

func (c *XOR) writeValue(v float64) {
	x := math.Float64bits(v) ^ math.Float64bits(c.lastV)
	if x == 0 {
		c.w.WriteBit(false)
		return
	}
	c.w.WriteBit(true)
	leading := uint8(bits.LeadingZeros64(x))
	trailing := uint8(bits.TrailingZeros64(x))
	if leading > 31 {
		leading = 31 // only 5 bits available
	}
	if c.leading != 0xff && leading >= c.leading && trailing >= c.trailing {
		c.w.WriteBit(false)
		sig := 64 - int(c.leading) - int(c.trailing)
		c.w.WriteBits(x>>c.trailing, sig)
		return
	}
	c.leading, c.trailing = leading, trailing
	c.w.WriteBit(true)
	c.w.WriteBits(uint64(leading), 5)
	sig := 64 - int(leading) - int(trailing)
	// sig is in [1, 64]; 64 does not fit in 6 bits and is stored as 0.
	c.w.WriteBits(uint64(sig)&0x3f, 6)
	c.w.WriteBits(x>>trailing, sig)
}

// Iterator returns a forward iterator over the chunk's samples. The iterator
// snapshots the sample count, so samples appended afterwards are not visited.
func (c *XOR) Iterator() *Iterator {
	return &Iterator{r: bitstream.NewReader(c.Bytes()), total: c.num}
}

// Iterator decodes samples from a chunk.
type Iterator struct {
	r        *bitstream.Reader
	total    int
	read     int
	t        int64
	v        float64
	tDelta   int64
	leading  uint8
	trailing uint8
	err      error
}

// Next advances to the next sample. It returns false at the end or on error.
func (it *Iterator) Next() bool {
	if it.err != nil || it.read >= it.total {
		return false
	}
	if it.read == 0 {
		t, err := it.r.ReadBits(64)
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.r.ReadBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t, it.v = int64(t), math.Float64frombits(v)
		it.read++
		return true
	}
	dod, err := it.readDoD()
	if err != nil {
		it.err = err
		return false
	}
	it.tDelta += dod
	it.t += it.tDelta
	if err := it.readValue(); err != nil {
		it.err = err
		return false
	}
	it.read++
	return true
}

func signExtend(v uint64, nbits uint) int64 {
	return int64(v<<(64-nbits)) >> (64 - nbits)
}

func (it *Iterator) readDoD() (int64, error) {
	var prefix int
	for prefix < 4 {
		b, err := it.r.ReadBit()
		if err != nil {
			return 0, err
		}
		if !b {
			break
		}
		prefix++
	}
	var nbits uint
	switch prefix {
	case 0:
		return 0, nil
	case 1:
		nbits = 14
	case 2:
		nbits = 17
	case 3:
		nbits = 20
	default:
		v, err := it.r.ReadBits(64)
		return int64(v), err
	}
	v, err := it.r.ReadBits(int(nbits))
	if err != nil {
		return 0, err
	}
	return signExtend(v, nbits), nil
}

func (it *Iterator) readValue() error {
	b, err := it.r.ReadBit()
	if err != nil {
		return err
	}
	if !b {
		return nil // value unchanged
	}
	b, err = it.r.ReadBit()
	if err != nil {
		return err
	}
	if b {
		lead, err := it.r.ReadBits(5)
		if err != nil {
			return err
		}
		sig, err := it.r.ReadBits(6)
		if err != nil {
			return err
		}
		if sig == 0 {
			sig = 64
		}
		it.leading = uint8(lead)
		it.trailing = uint8(64 - lead - sig)
	}
	sig := 64 - int(it.leading) - int(it.trailing)
	x, err := it.r.ReadBits(sig)
	if err != nil {
		return err
	}
	it.v = math.Float64frombits(math.Float64bits(it.v) ^ (x << it.trailing))
	return nil
}

// At returns the current sample.
func (it *Iterator) At() (int64, float64) { return it.t, it.v }

// T returns the current timestamp.
func (it *Iterator) T() int64 { return it.t }

// Err returns the first decoding error, if any.
func (it *Iterator) Err() error { return it.err }
