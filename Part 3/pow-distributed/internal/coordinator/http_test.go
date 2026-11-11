package coordinator

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c, err := New(Config{ChunkSize: 1000, HeartbeatTimeout: time.Second, AssignmentTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func TestREST(t *testing.T) {
	c := newTestCoordinator(t)
	for _, body := range []string{`{}`, `{"data":"" ,"difficulty":1}`, `{"data":"a"}`, `{"data":"a","difficulty":0}`, `{"data":"a","difficulty":9}`, `{"data":"a","difficulty":1.5}`, `x`, `{"data":"a","difficulty":1} {}`, `{"data":"a","difficulty":null}`} {
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/jobs", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatalf("%s => %d", body, w.Code)
		}
	}
	w := httptest.NewRecorder()
	c.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"data":"hello","difficulty":4}`)))
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	var result struct {
		ID string `json:"job_id"`
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/jobs", 200}, {"GET", "/workers", 200}, {"GET", "/jobs/missing", 404}, {"DELETE", "/jobs/missing", 404}, {"GET", "/jobs/" + result.ID, 200}, {"DELETE", "/jobs/" + result.ID, 200}, {"DELETE", "/jobs/" + result.ID, 409}} {
		r := httptest.NewRecorder()
		c.Handler().ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.code {
			t.Fatalf("%+v => %d", tc, r.Code)
		}
	}
	r := httptest.NewRecorder()
	c.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/jobs/"+result.ID+"/events", nil))
	if !strings.Contains(r.Body.String(), "event: cancelled") {
		t.Fatal(r.Body)
	}
}
