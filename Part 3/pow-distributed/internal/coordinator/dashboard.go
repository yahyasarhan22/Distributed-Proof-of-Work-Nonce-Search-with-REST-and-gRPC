package coordinator

import (
	"embed"
	"net/http"
)

//go:embed web/*
var dashboard embed.FS

func serveDashboard(w http.ResponseWriter, r *http.Request) {
	name := "index.html"
	contentType := "text/html; charset=utf-8"
	if r.URL.Path != "/" {
		name = r.PathValue("name")
		switch name {
		case "app.js":
			contentType = "text/javascript; charset=utf-8"
		case "style.css":
			contentType = "text/css; charset=utf-8"
		default:
			http.NotFound(w, r)
			return
		}
	}
	data, err := dashboard.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	_, _ = w.Write(data)
}
