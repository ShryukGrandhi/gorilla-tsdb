package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ShryukGrandhi/gorilla-tsdb/tsdb"
)

func setup(t *testing.T) (*httptest.Server, *tsdb.DB) {
	t.Helper()
	db, err := tsdb.Open(tsdb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	now := func() time.Time { return time.UnixMilli(600_000) }
	srv := httptest.NewServer(New(db, now))
	t.Cleanup(srv.Close)
	return srv, db
}

func decode(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("bad json %q: %v", b, err)
	}
	return out
}

func TestWriteThenQuery(t *testing.T) {
	srv, _ := setup(t)
	var body strings.Builder
	for ts := 0; ts <= 300; ts += 15 {
		fmt.Fprintf(&body, "req_total{host=\"a\"} %d %d\n", ts*2, ts*1000)
		fmt.Fprintf(&body, "req_total{host=\"b\"} %d %d\n", ts, ts*1000)
	}
	body.WriteString("up 1\n") // no timestamp: server clock (600s)
	resp, err := http.Post(srv.URL+"/api/v1/write", "text/plain", strings.NewReader(body.String()))
	if err != nil {
		t.Fatal(err)
	}
	out := decode(t, resp)
	if resp.StatusCode != 200 || out["data"].(map[string]any)["accepted"].(float64) != 43 {
		t.Fatalf("write: %d %v", resp.StatusCode, out)
	}

	resp, _ = http.Get(srv.URL + "/api/v1/query?" + url.Values{"query": {"sum(rate(req_total[1m]))"}, "time": {"300"}}.Encode())
	out = decode(t, resp)
	res := out["data"].(map[string]any)["result"].([]any)
	if len(res) != 1 || res[0].(map[string]any)["value"].([]any)[1] != "3" {
		t.Fatalf("query: %v", out)
	}

	resp, _ = http.Get(srv.URL + "/api/v1/query?query=up")
	out = decode(t, resp)
	if res := out["data"].(map[string]any)["result"].([]any); len(res) != 1 {
		t.Fatalf("query without time: %v", out)
	}

	resp, _ = http.Get(srv.URL + "/api/v1/query_range?" + url.Values{
		"query": {`rate(req_total{host="a"}[1m])`}, "start": {"60"}, "end": {"300"}, "step": {"1m"}}.Encode())
	out = decode(t, resp)
	data := out["data"].(map[string]any)
	if data["resultType"] != "matrix" {
		t.Fatalf("range: %v", out)
	}
	vals := data["result"].([]any)[0].(map[string]any)["values"].([]any)
	if len(vals) != 5 || vals[0].([]any)[1] != "2" {
		t.Fatalf("range values: %v", vals)
	}

	resp, _ = http.Get(srv.URL + "/api/v1/label/host/values")
	out = decode(t, resp)
	if fmt.Sprint(out["data"]) != "[a b]" {
		t.Fatalf("label values: %v", out)
	}
	resp, _ = http.Get(srv.URL + "/api/v1/status")
	out = decode(t, resp)
	if out["data"].(map[string]any)["series"].(float64) != 3 {
		t.Fatalf("status: %v", out)
	}
}

func TestBadRequests(t *testing.T) {
	srv, _ := setup(t)
	resp, _ := http.Post(srv.URL+"/api/v1/write", "text/plain", strings.NewReader("ok_metric 1 1000\nnot valid{\n"))
	out := decode(t, resp)
	if resp.StatusCode != 400 || !strings.Contains(out["error"].(string), "line 2") {
		t.Fatalf("write: %d %v", resp.StatusCode, out)
	}
	if out["data"].(map[string]any)["accepted"].(float64) != 1 {
		t.Fatalf("lines before the error should be kept: %v", out)
	}
	for _, path := range []string{
		"/api/v1/query",
		"/api/v1/query?query=rate(x)",
		"/api/v1/query?query=up&time=abc",
		"/api/v1/query_range?query=up&start=0&end=10",
		"/api/v1/query_range?query=up&start=0&end=10&step=0",
	} {
		resp, _ := http.Get(srv.URL + path)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", path, resp.StatusCode)
		}
	}
}
