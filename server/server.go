// Package server exposes the database over HTTP with a JSON API modelled on
// the Prometheus HTTP API.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/lineproto"
	"github.com/ShryukGrandhi/gorilla-tsdb/query"
	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

// MaxBodyBytes limits the size of a write request.
const MaxBodyBytes = 32 << 20

const commitEvery = 5000

// Server wires HTTP handlers to a DB and a query engine.
type Server struct {
	db     *tsdb.DB
	engine *query.Engine
	now    func() time.Time
	mux    *http.ServeMux
}

// New returns a Server. now may be nil to use the wall clock.
func New(db *tsdb.DB, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	s := &Server{db: db, engine: query.NewEngine(db), now: now, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/v1/write", s.handleWrite)
	s.mux.HandleFunc("GET /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("GET /api/v1/query_range", s.handleQueryRange)
	s.mux.HandleFunc("POST /api/v1/query_range", s.handleQueryRange)
	s.mux.HandleFunc("GET /api/v1/labels", s.handleLabels)
	s.mux.HandleFunc("GET /api/v1/label/{name}/values", s.handleLabelValues)
	s.mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type response struct {
	Status string `json:"status"`
	Data   any    `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, response{Status: "error", Error: err.Error()})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	app := s.db.Appender()
	now := s.now().UnixMilli()
	var total tsdb.CommitResult
	pending := 0
	commit := func() error {
		res, err := app.Commit()
		total.Accepted += res.Accepted
		total.OutOfOrder += res.OutOfOrder
		pending = 0
		return err
	}
	err := lineproto.Parse(body, func(e lineproto.Entry) error {
		t := now
		if e.HasT {
			t = e.T
		}
		if err := app.Add(e.Labels, t, e.Value); err != nil {
			return err
		}
		pending++
		if pending >= commitEvery {
			return commit()
		}
		return nil
	})
	if err == nil {
		err = commit()
	} else {
		// Lines before the bad one are kept so a client can resume after it.
		_ = commit()
	}
	data := map[string]int{"accepted": total.Accepted, "out_of_order": total.OutOfOrder}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, response{Status: "error", Error: err.Error(), Data: data})
		return
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: data})
}

// parseTime accepts Unix seconds with an optional fraction.
func parseTime(s string, def time.Time) (int64, error) {
	if s == "" {
		return def.UnixMilli(), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("invalid time %q", s)
	}
	return int64(math.Round(f * 1000)), nil
}

// parseStep accepts seconds ("15", "0.5") or a duration ("15s", "1m").
func parseStep(s string) (int64, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f * 1000), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid step %q", s)
	}
	return d.Milliseconds(), nil
}

func formatValue(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func pair(t int64, v float64) [2]any { return [2]any{float64(t) / 1000, formatValue(v)} }

type vectorItem struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

type matrixItem struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

func encodeValue(v query.Value) map[string]any {
	switch r := v.(type) {
	case query.Scalar:
		return map[string]any{"resultType": "scalar", "result": pair(r.T, r.V)}
	case query.Vector:
		items := make([]vectorItem, 0, len(r))
		for _, s := range r {
			items = append(items, vectorItem{Metric: s.Labels.Map(), Value: pair(s.T, s.V)})
		}
		return map[string]any{"resultType": "vector", "result": items}
	case query.Matrix:
		items := make([]matrixItem, 0, len(r))
		for _, s := range r {
			vals := make([][2]any, len(s.Points))
			for i, p := range s.Points {
				vals[i] = pair(p.T, p.V)
			}
			items = append(items, matrixItem{Metric: s.Labels.Map(), Values: vals})
		}
		return map[string]any{"resultType": "matrix", "result": items}
	}
	return nil
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.FormValue("query")
	if q == "" {
		fail(w, http.StatusBadRequest, errors.New("missing query parameter"))
		return
	}
	ts, err := parseTime(r.FormValue("time"), s.now())
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	v, err := s.engine.Instant(q, ts)
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: encodeValue(v)})
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.FormValue("query")
	start, err1 := parseTime(r.FormValue("start"), time.Time{})
	end, err2 := parseTime(r.FormValue("end"), time.Time{})
	step, err3 := parseStep(r.FormValue("step"))
	if err := errors.Join(err1, err2, err3); err != nil || q == "" || r.FormValue("start") == "" || r.FormValue("end") == "" {
		if err == nil {
			err = errors.New("query, start, end and step are required")
		}
		fail(w, http.StatusBadRequest, err)
		return
	}
	m, err := s.engine.Range(q, start, end, step)
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, response{Status: "success", Data: encodeValue(m)})
}

func (s *Server) handleLabels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, response{Status: "success", Data: s.db.LabelNames()})
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, response{Status: "success", Data: s.db.LabelValues(r.PathValue("name"))})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.db.Stats()
	writeJSON(w, http.StatusOK, response{Status: "success", Data: map[string]any{
		"series":           st.Series,
		"chunks":           st.Chunks,
		"samples":          st.Samples,
		"chunk_bytes":      st.ChunkBytes,
		"bytes_per_sample": st.BytesPerSample,
		"appended_total":   st.Appended,
		"out_of_order":     st.OutOfOrder,
	}})
}
