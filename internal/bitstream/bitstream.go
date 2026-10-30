// Package bitstream provides a minimal MSB-first bit writer and reader used by
// the Gorilla chunk encoder.
package bitstream

import "errors"

// ErrEOF is returned when a read runs past the end of the stream.
var ErrEOF = errors.New("bitstream: unexpected end of stream")

// Writer appends bits to a growing byte slice, most significant bit first.
type Writer struct {
	b    []byte
	used uint8 // bits used in the last byte; 8 means a new byte is needed
}

// NewWriter returns an empty writer. capHint pre-sizes the backing buffer.
func NewWriter(capHint int) *Writer {
	return &Writer{b: make([]byte, 0, capHint), used: 8}
}

// WriteBit appends a single bit.
func (w *Writer) WriteBit(bit bool) {
	if w.used == 8 {
		w.b = append(w.b, 0)
		w.used = 0
	}
	if bit {
		w.b[len(w.b)-1] |= 1 << (7 - w.used)
	}
	w.used++
}

// WriteBits appends the low nbits of v, most significant first. nbits must be
// in [0, 64].
func (w *Writer) WriteBits(v uint64, nbits int) {
	for nbits > 0 {
		if w.used == 8 {
			w.b = append(w.b, 0)
			w.used = 0
		}
		free := 8 - int(w.used)
		take := free
		if nbits < take {
			take = nbits
		}
		chunk := byte((v >> uint(nbits-take)) & (1<<uint(take) - 1))
		w.b[len(w.b)-1] |= chunk << uint(free-take)
		w.used += uint8(take)
		nbits -= take
	}
}

// Bytes returns the underlying buffer. The final byte may be partially used.
// The returned slice aliases the writer's storage.
func (w *Writer) Bytes() []byte { return w.b }

// BitLen returns the number of bits written so far.
func (w *Writer) BitLen() int {
	if len(w.b) == 0 {
		return 0
	}
	return (len(w.b)-1)*8 + int(w.used)
}

// Reader reads bits from a byte slice, most significant bit first.
type Reader struct {
	b   []byte
	pos int // absolute bit position
}

// NewReader returns a reader over b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// ReadBit reads a single bit.
func (r *Reader) ReadBit() (bool, error) {
	if r.pos >= len(r.b)*8 {
		return false, ErrEOF
	}
	bit := r.b[r.pos>>3]&(1<<(7-uint(r.pos&7))) != 0
	r.pos++
	return bit, nil
}

// ReadBits reads nbits (0..64) and returns them right-aligned.
func (r *Reader) ReadBits(nbits int) (uint64, error) {
	if r.pos+nbits > len(r.b)*8 {
		return 0, ErrEOF
	}
	var v uint64
	for nbits > 0 {
		off := r.pos & 7
		avail := 8 - off
		take := avail
		if nbits < take {
			take = nbits
		}
		cur := uint64(r.b[r.pos>>3]>>uint(avail-take)) & (1<<uint(take) - 1)
		v = v<<uint(take) | cur
		r.pos += take
		nbits -= take
	}
	return v, nil
}
