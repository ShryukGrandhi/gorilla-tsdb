package tsdb

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/ShryukGrandhi/gorilla-tsdb/index"
	"github.com/ShryukGrandhi/gorilla-tsdb/labels"
)

// Record types stored in the WAL and in checkpoints.
const (
	recSeries  byte = 1
	recSamples byte = 2
	recChunk   byte = 3
)

var errBadRecord = errors.New("tsdb: malformed record")

type refSample struct {
	ref index.SeriesRef
	t   int64
	v   float64
}

func encodeSeries(buf []byte, ref index.SeriesRef, ls labels.Labels) []byte {
	buf = append(buf[:0], recSeries)
	buf = binary.AppendUvarint(buf, uint64(ref))
	buf = binary.AppendUvarint(buf, uint64(len(ls)))
	for _, l := range ls {
		buf = binary.AppendUvarint(buf, uint64(len(l.Name)))
		buf = append(buf, l.Name...)
		buf = binary.AppendUvarint(buf, uint64(len(l.Value)))
		buf = append(buf, l.Value...)
	}
	return buf
}

// encodeSamples stores timestamps as deltas from the first sample, which
// keeps most varints at two or three bytes.
func encodeSamples(buf []byte, ss []refSample) []byte {
	buf = append(buf[:0], recSamples)
	buf = binary.AppendUvarint(buf, uint64(len(ss)))
	if len(ss) == 0 {
		return buf
	}
	base := ss[0].t
	buf = binary.AppendVarint(buf, base)
	for _, s := range ss {
		buf = binary.AppendUvarint(buf, uint64(s.ref))
		buf = binary.AppendVarint(buf, s.t-base)
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(s.v))
	}
	return buf
}

func encodeChunk(buf []byte, ref index.SeriesRef, num int, data []byte) []byte {
	buf = append(buf[:0], recChunk)
	buf = binary.AppendUvarint(buf, uint64(ref))
	buf = binary.AppendUvarint(buf, uint64(num))
	buf = binary.AppendUvarint(buf, uint64(len(data)))
	return append(buf, data...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errBadRecord
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errBadRecord
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes(n uint64) []byte {
	if d.err != nil {
		return nil
	}
	if uint64(len(d.b)) < n {
		d.err = errBadRecord
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) str() string { return string(d.bytes(d.uvarint())) }

func decodeSeries(rec []byte) (index.SeriesRef, labels.Labels, error) {
	d := decoder{b: rec[1:]}
	ref := index.SeriesRef(d.uvarint())
	n := d.uvarint()
	if n > uint64(len(rec)) {
		return 0, nil, errBadRecord
	}
	ls := make(labels.Labels, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		name := d.str()
		value := d.str()
		ls = append(ls, labels.Label{Name: name, Value: value})
	}
	return ref, ls, d.err
}

func decodeSamples(rec []byte, out []refSample) ([]refSample, error) {
	d := decoder{b: rec[1:]}
	n := d.uvarint()
	if n > uint64(len(rec)) {
		return nil, errBadRecord
	}
	out = out[:0]
	if n == 0 {
		return out, d.err
	}
	base := d.varint()
	for i := uint64(0); i < n && d.err == nil; i++ {
		ref := index.SeriesRef(d.uvarint())
		t := base + d.varint()
		vb := d.bytes(8)
		if d.err != nil {
			break
		}
		out = append(out, refSample{ref, t, math.Float64frombits(binary.BigEndian.Uint64(vb))})
	}
	return out, d.err
}

func decodeChunk(rec []byte) (index.SeriesRef, int, []byte, error) {
	d := decoder{b: rec[1:]}
	ref := index.SeriesRef(d.uvarint())
	num := int(d.uvarint())
	data := d.bytes(d.uvarint())
	return ref, num, data, d.err
}
