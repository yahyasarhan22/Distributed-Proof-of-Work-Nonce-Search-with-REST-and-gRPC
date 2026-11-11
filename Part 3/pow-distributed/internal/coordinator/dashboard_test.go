package coordinator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardAndAPICoexist(t *testing.T) {
	c := newTestCoordinator(t)
	for _, tc := range []struct {
		path, contentType string
		code              int
	}{{"/", "text/html", 200}, {"/assets/app.js", "text/javascript", 200}, {"/assets/style.css", "text/css", 200}, {"/jobs", "application/json", 200}, {"/workers", "application/json", 200}, {"/does-not-exist", "", 404}, {"/assets/missing.js", "", 404}} {
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Code != tc.code || (tc.contentType != "" && !strings.Contains(w.Header().Get("Content-Type"), tc.contentType)) {
			t.Fatalf("%s => %d %v", tc.path, w.Code, w.Header())
		}
		if tc.path == "/" && !strings.Contains(w.Body.String(), "<form") {
			t.Fatal("dashboard form not served")
		}
	}
}
