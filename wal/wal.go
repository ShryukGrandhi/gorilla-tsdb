// Package wal implements a segmented, checksummed write-ahead log.
//
// Each segment is a file named by an 8-digit index. A segment is a sequence
// of records framed as:
//
//	length  uint32 big-endian (payload bytes)
//	crc     uint32 big-endian (CRC-32C of payload)
//	payload length bytes
//
// On replay a short or checksum-failing record at the end of the final
// segment is treated as a torn write from a crash: the tail is dropped and the
// log continues. Corruption anywhere else is reported as an error.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const headerSize = 8

// MaxRecordSize bounds a single record to catch garbage lengths on replay.
const MaxRecordSize = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt reports a damaged record outside the tail of the final segment.
var ErrCorrupt = errors.New("wal: corrupt record")

// Options configures a WAL.
type Options struct {
	// SegmentSize is the size after which a new segment is started.
	SegmentSize int64
	// Sync fsyncs after every Log call. Without it data reaches the OS page
	// cache on every Log call but is only fsynced on Cut and Close.
	Sync bool
}

// WAL is an append-only log. It is safe for concurrent use.
type WAL struct {
	mu      sync.Mutex
	dir     string
	opts    Options
	f       *os.File
	bw      *bufio.Writer
	segIdx  int
	segSize int64
	hdr     [headerSize]byte
	closed  bool
}

// Open opens or creates a WAL in dir. Writing always starts in a fresh
// segment after the highest existing one, so existing segments are never
// modified.
func Open(dir string, opts Options) (*WAL, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 128 << 20
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	next := 1
	if len(segs) > 0 {
		next = segs[len(segs)-1] + 1
	}
	w := &WAL{dir: dir, opts: opts}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	return w, nil
}

// SegmentName returns the file name for a segment index.
func SegmentName(dir string, idx int) string {
	return filepath.Join(dir, fmt.Sprintf("%08d", idx))
}

func (w *WAL) openSegment(idx int) error {
	f, err := os.OpenFile(SegmentName(w.dir, idx), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	w.bw = bufio.NewWriterSize(f, 256<<10)
	w.segIdx = idx
	w.segSize = 0
	return nil
}

func (w *WAL) closeSegment() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// Log appends records atomically with respect to other Log calls and flushes
// them to the operating system before returning.
func (w *WAL) Log(recs ...[]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: closed")
	}
	for _, rec := range recs {
		if len(rec) > MaxRecordSize {
			return fmt.Errorf("wal: record of %d bytes exceeds limit", len(rec))
		}
		if w.segSize > 0 && w.segSize+int64(headerSize+len(rec)) > w.opts.SegmentSize {
			if err := w.cutLocked(); err != nil {
				return err
			}
		}
		binary.BigEndian.PutUint32(w.hdr[0:4], uint32(len(rec)))
		binary.BigEndian.PutUint32(w.hdr[4:8], crc32.Checksum(rec, castagnoli))
		if _, err := w.bw.Write(w.hdr[:]); err != nil {
			return err
		}
		if _, err := w.bw.Write(rec); err != nil {
			return err
		}
		w.segSize += int64(headerSize + len(rec))
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if w.opts.Sync {
		return w.f.Sync()
	}
	return nil
}

func (w *WAL) cutLocked() error {
	if err := w.closeSegment(); err != nil {
		return err
	}
	return w.openSegment(w.segIdx + 1)
}

// Cut closes the current segment and starts a new one, returning the new
// segment's index. Every record logged before Cut returns lives in a segment
// with a smaller index.
func (w *WAL) Cut() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.cutLocked(); err != nil {
		return 0, err
	}
	return w.segIdx, nil
}

// WriteRecord writes one framed record to w. It is used to build standalone
// files (checkpoints) that ReadFile can read back.
func WriteRecord(w io.Writer, rec []byte) error {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(rec)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(rec, castagnoli))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(rec)
	return err
}

// Dir returns the WAL directory.
func (w *WAL) Dir() string { return w.dir }

// TruncateBefore deletes all segments with an index lower than idx.
func (w *WAL) TruncateBefore(idx int) error {
	segs, err := Segments(w.dir)
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s >= idx {
			break
		}
		if err := os.Remove(SegmentName(w.dir, s)); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes, syncs and closes the log.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.closeSegment()
}

// Segments lists segment indexes in dir in ascending order.
func Segments(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []int
	for _, e := range entries {
		if e.IsDir() || len(e.Name()) != 8 {
			continue
		}
		n, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out, nil
}

// ReplayStats describes what a replay read.
type ReplayStats struct {
	Segments     int
	Records      int
	Bytes        int64
	TornTailDrop bool // a partial record at the end of the log was discarded
}

// Replay calls fn for every record in segments with index >= from, in order.
// The slice passed to fn is only valid for the duration of the call.
func Replay(dir string, from int, fn func(rec []byte) error) (ReplayStats, error) {
	var st ReplayStats
	segs, err := Segments(dir)
	if err != nil {
		return st, err
	}
	var todo []int
	for _, s := range segs {
		if s >= from {
			todo = append(todo, s)
		}
	}
	for i, s := range todo {
		last := i == len(todo)-1
		torn, err := replaySegment(SegmentName(dir, s), last, fn, &st)
		if err != nil {
			return st, fmt.Errorf("segment %08d: %w", s, err)
		}
		st.Segments++
		if torn {
			st.TornTailDrop = true
		}
	}
	return st, nil
}

// ReadFile replays a single standalone file (used for checkpoints, which share
// the record framing). Any damage is an error.
func ReadFile(path string, fn func(rec []byte) error) error {
	var st ReplayStats
	_, err := replaySegment(path, false, fn, &st)
	return err
}

func replaySegment(path string, last bool, fn func([]byte) error, st *ReplayStats) (torn bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	var hdr [headerSize]byte
	buf := make([]byte, 0, 4096)
	for {
		n, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return false, nil
		}
		if err != nil { // partial header
			if last && n > 0 {
				return true, nil
			}
			return false, fmt.Errorf("%w: short header", ErrCorrupt)
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		sum := binary.BigEndian.Uint32(hdr[4:8])
		if length > MaxRecordSize {
			if last {
				return true, nil
			}
			return false, fmt.Errorf("%w: length %d", ErrCorrupt, length)
		}
		if cap(buf) < int(length) {
			buf = make([]byte, length)
		}
		buf = buf[:length]
		if _, err := io.ReadFull(r, buf); err != nil {
			if last {
				return true, nil
			}
			return false, fmt.Errorf("%w: short payload", ErrCorrupt)
		}
		if crc32.Checksum(buf, castagnoli) != sum {
			if last && atEOF(r) {
				return true, nil
			}
			return false, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
		}
		st.Records++
		st.Bytes += int64(headerSize) + int64(length)
		if err := fn(buf); err != nil {
			return false, err
		}
	}
}

func atEOF(r *bufio.Reader) bool {
	_, err := r.Peek(1)
	return err == io.EOF
}
