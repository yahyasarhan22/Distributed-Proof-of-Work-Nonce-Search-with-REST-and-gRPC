package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func jsonResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func apiError(w http.ResponseWriter, code int, err error) {
	jsonResponse(w, code, map[string]string{"error": err.Error()})
}
func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", serveDashboard)
	mux.HandleFunc("GET /assets/{name}", serveDashboard)
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var in struct {
			Data       string `json:"data"`
			Difficulty *int   `json:"difficulty"`
		}
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if err := d.Decode(&in); err != nil {
			apiError(w, 400, errors.New("invalid JSON job"))
			return
		}
		var extra any
		if d.Decode(&extra) != io.EOF || in.Difficulty == nil {
			apiError(w, 400, errors.New("one JSON object with difficulty required"))
			return
		}
		j, err := c.Submit(in.Data, *in.Difficulty)
		if err != nil {
			apiError(w, 400, err)
			return
		}
		jsonResponse(w, 202, map[string]string{"job_id": j.JobID})
	})
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, c.Jobs()) })
	mux.HandleFunc("GET /workers", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, c.Workers()) })
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		j, ok := c.Get(r.PathValue("id"))
		if !ok {
			apiError(w, 404, ErrNotFound)
			return
		}
		jsonResponse(w, 200, j)
	})
	mux.HandleFunc("DELETE /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		err := c.Cancel(r.PathValue("id"))
		if err != nil {
			code := 409
			if errors.Is(err, ErrNotFound) {
				code = 404
			}
			apiError(w, code, err)
			return
		}
		j, _ := c.Get(r.PathValue("id"))
		jsonResponse(w, 200, j)
	})
	mux.HandleFunc("GET /jobs/{id}/events", c.events)
	return mux
}
func (c *Coordinator) events(w http.ResponseWriter, r *http.Request) {
	ch, unsubscribe, ok := c.Subscribe(r.PathValue("id"))
	if !ok {
		apiError(w, 404, ErrNotFound)
		return
	}
	defer unsubscribe()
	flusher, ok := w.(http.Flusher)
	if !ok {
		apiError(w, 500, errors.New("streaming unavailable"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var v JobView
		select {
		case <-r.Context().Done():
			return
		case v = <-ch:
		case <-ticker.C:
			v, _ = c.Get(r.PathValue("id"))
		}
		name := "progress"
		if v.Status != "RUNNING" {
			name = strings.ToLower(v.Status)
		}
		b, err := json.Marshal(v)
		if err != nil {
			return
		}
		// A connected client that stops reading must not hold a handler forever.
		controller := http.NewResponseController(w)
		_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b); err != nil {
			return
		}
		flusher.Flush()
		if v.Status != "RUNNING" {
			return
		}
	}
}
