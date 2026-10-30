package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
)

func collect(t *testing.T, dir string, from int) ([][]byte, ReplayStats) {
	t.Helper()
	var out [][]byte
	st, err := Replay(dir, from, func(rec []byte) error {
		out = append(out, append([]byte(nil), rec...))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out, st
}

func TestLogAndReplayAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SegmentSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	var want [][]byte
	for i := 0; i < 100; i++ {
		rec := []byte(fmt.Sprintf("record-%03d", i))
		want = append(want, rec)
		if err := w.Log(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := Segments(dir)
	if len(segs) < 5 {
		t.Fatalf("expected rotation into several segments, got %d", len(segs))
	}
	got, st := collect(t, dir, 0)
	if len(got) != len(want) || st.Records != len(want) {
		t.Fatalf("replayed %d records", len(got))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d: %q != %q", i, got[i], want[i])
		}
	}
}

func TestReopenStartsNewSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{})
	_ = w.Log([]byte("a"))
	_ = w.Close()
	w, _ = Open(dir, Options{})
	_ = w.Log([]byte("b"))
	_ = w.Close()
	segs, _ := Segments(dir)
	if len(segs) != 2 {
		t.Fatalf("segments = %v", segs)
	}
	got, _ := collect(t, dir, 0)
	if len(got) != 2 || string(got[0]) != "a" || string(got[1]) != "b" {
		t.Fatalf("got %q", got)
	}
}

func TestTornTailIsDropped(t *testing.T) {
	for _, cut := range []int{1, 5, 8, 12} {
		dir := t.TempDir()
		w, _ := Open(dir, Options{})
		_ = w.Log([]byte("first"), []byte("second-record"))
		_ = w.Close()
		path := SegmentName(dir, 1)
		info, _ := os.Stat(path)
		if err := os.Truncate(path, info.Size()-int64(cut)); err != nil {
			t.Fatal(err)
		}
		got, st := collect(t, dir, 0)
		if len(got) != 1 || string(got[0]) != "first" || !st.TornTailDrop {
			t.Fatalf("cut=%d: got %q torn=%v", cut, got, st.TornTailDrop)
		}
	}
}

func TestCorruptionInOlderSegmentIsError(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{})
	_ = w.Log([]byte("aaaa"), []byte("bbbb"))
	_, _ = w.Cut()
	_ = w.Log([]byte("cccc"))
	_ = w.Close()
	path := SegmentName(dir, 1)
	b, _ := os.ReadFile(path)
	b[headerSize] ^= 0xff // flip a payload byte of the first record
	_ = os.WriteFile(path, b, 0o644)
	_, err := Replay(dir, 0, func([]byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCorruptionMidFinalSegmentIsError(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{})
	_ = w.Log([]byte("aaaa"), []byte("bbbb"))
	_ = w.Close()
	path := SegmentName(dir, 1)
	b, _ := os.ReadFile(path)
	b[headerSize] ^= 0xff
	_ = os.WriteFile(path, b, 0o644)
	_, err := Replay(dir, 0, func([]byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestTruncateBefore(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{})
	_ = w.Log([]byte("old"))
	idx, _ := w.Cut()
	_ = w.Log([]byte("new"))
	_ = w.Close()
	if err := w.TruncateBefore(idx); err != nil {
		t.Fatal(err)
	}
	got, _ := collect(t, dir, 0)
	if len(got) != 1 || string(got[0]) != "new" {
		t.Fatalf("got %q", got)
	}
}
