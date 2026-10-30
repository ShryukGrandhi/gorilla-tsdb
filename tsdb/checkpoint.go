package tsdb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ShryukGrandhi/gorilla-tsdb/chunk"
	"github.com/ShryukGrandhi/gorilla-tsdb/wal"
)

// CheckpointStats describes a completed checkpoint.
type CheckpointStats struct {
	Segment int
	Series  int
	Chunks  int
	Bytes   int64
}

// Checkpoint writes the current head to a checkpoint file and deletes the WAL
// segments it covers, bounding recovery time.
//
// The WAL is cut under the exclusive gc lock, so every sample logged in an
// older segment has already been applied to memory when the snapshot starts.
// Samples appended while the snapshot is being taken land in the new segment
// and may also appear in the checkpoint; replay drops them as duplicates
// because they are not newer than the series' last timestamp.
func (db *DB) Checkpoint() (CheckpointStats, error) {
	var cs CheckpointStats
	if db.wal == nil {
		return cs, nil
	}
	db.ckptMu.Lock()
	defer db.ckptMu.Unlock()

	db.gcMu.Lock()
	idx, err := db.wal.Cut()
	db.gcMu.Unlock()
	if err != nil {
		return cs, err
	}
	cs.Segment = idx

	final := filepath.Join(db.opts.Dir, fmt.Sprintf("%s%08d", checkpointPrefix, idx))
	tmp := final + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return cs, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var buf []byte
	write := func(rec []byte) error {
		cs.Bytes += int64(len(rec) + 8)
		return wal.WriteRecord(bw, rec)
	}
	for i := range db.stripes {
		st := &db.stripes[i]
		st.mu.RLock()
		series := make([]*memSeries, 0, len(st.byRef))
		for _, s := range st.byRef {
			series = append(series, s)
		}
		st.mu.RUnlock()
		for _, s := range series {
			s.mu.Lock()
			if s.deleted {
				s.mu.Unlock()
				continue
			}
			buf = encodeSeries(buf, s.ref, s.lset)
			err = write(buf)
			for _, c := range s.chunks {
				if err != nil {
					break
				}
				buf = encodeChunk(buf, s.ref, c.NumSamples(), c.Bytes())
				err = write(buf)
				cs.Chunks++
			}
			s.mu.Unlock()
			if err != nil {
				f.Close()
				return cs, err
			}
			cs.Series++
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return cs, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return cs, err
	}
	if err := f.Close(); err != nil {
		return cs, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return cs, err
	}
	if err := syncDir(db.opts.Dir); err != nil {
		return cs, err
	}
	// The new checkpoint is durable; older state can go.
	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return cs, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, checkpointPrefix) && filepath.Join(db.opts.Dir, name) != final {
			if err := os.Remove(filepath.Join(db.opts.Dir, name)); err != nil {
				return cs, err
			}
		}
	}
	return cs, db.wal.TruncateBefore(idx)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (db *DB) loadCheckpoint(path string) error {
	return wal.ReadFile(path, func(rec []byte) error {
		if len(rec) == 0 {
			return errBadRecord
		}
		switch rec[0] {
		case recSeries:
			ref, ls, err := decodeSeries(rec)
			if err != nil {
				return err
			}
			db.createWithRef(ref, ls)
			db.recov.CheckpointSeries++
		case recChunk:
			ref, num, data, err := decodeChunk(rec)
			if err != nil {
				return err
			}
			s := db.getByRef(ref)
			if s == nil {
				return fmt.Errorf("chunk for unknown series %d", ref)
			}
			c, err := chunk.LoadXOR(append([]byte(nil), data...), num)
			if err != nil {
				return err
			}
			if num == 0 {
				return nil
			}
			s.chunks = append(s.chunks, c)
			s.lastT = c.MaxT()
			db.recov.CheckpointChunks++
		default:
			return fmt.Errorf("unknown checkpoint record type %d", rec[0])
		}
		return nil
	})
}
