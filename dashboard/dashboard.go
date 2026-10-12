// Package dashboard exposes the BaQueue dashboard as a standard HTTP handler.
package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

//go:embed static/*
var assets embed.FS

type config struct {
	ctx              context.Context
	prefix           string
	auth             func(http.Handler) http.Handler
	origins          []string
	overviewInterval time.Duration
	jobsInterval     time.Duration
}

// Option configures the dashboard handler.
type Option func(*config)

// WithContext binds live feeds to ctx. Cancellation closes WebSockets, stops
// the overview broadcaster, and rejects new WebSocket handshakes. REST requests
// retain their own contexts so http.Server.Shutdown can drain them gracefully.
// The caller still owns the driver's lifecycle and must wait for handlers to
// return before disconnecting it.
func WithContext(ctx context.Context) Option {
	if ctx == nil {
		panic("dashboard: nil lifecycle context")
	}
	return func(c *config) { c.ctx = ctx }
}

// WithPrefix mounts every route under prefix (for example, "/baqueue").
// Register the returned handler at prefix and prefix+"/" without StripPrefix.
// Alternatively, use http.StripPrefix with New(driver) and no WithPrefix.
func WithPrefix(prefix string) Option {
	return func(c *config) {
		prefix = strings.TrimRight(prefix, "/")
		if prefix != "" && (!strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix || strings.ContainsAny(prefix, "?#%\\\r\n")) {
			panic("dashboard: invalid prefix")
		}
		c.prefix = prefix
	}
}

// WithAuth wraps all routes, including assets and WebSocket handshakes.
// The middleware must preserve http.Hijacker support for WebSocket requests.
func WithAuth(middleware func(http.Handler) http.Handler) Option {
	return func(c *config) { c.auth = middleware }
}

// WithOriginPatterns permits additional WebSocket origin hosts using
// coder/websocket's host-pattern syntax. By default only the request host is
// accepted. Never use "*" for a dashboard authenticated with cookies.
func WithOriginPatterns(patterns ...string) Option {
	return func(c *config) { c.origins = append([]string(nil), patterns...) }
}

// WithRefreshIntervals overrides the default 2s overview and 1.5s jobs feeds.
func WithRefreshIntervals(overview, jobs time.Duration) Option {
	return func(c *config) {
		if overview <= 0 || jobs <= 0 {
			panic("dashboard: refresh intervals must be positive")
		}
		c.overviewInterval, c.jobsInterval = overview, jobs
	}
}

type server struct {
	driver baqueue.Driver
	config config
	hub    overviewHub
}

func (s *server) lifecycleContext() context.Context {
	if s.config.ctx != nil {
		return s.config.ctx
	}
	return context.Background()
}

// New returns a handler usable with net/http, Gin's WrapH or Echo's WrapHandler.
// The caller owns the driver's connection and lifecycle. New starts no goroutines;
// live-feed goroutines run only while clients are connected. Configure WithAuth
// before exposing this administrative handler outside a trusted network.
func New(driver baqueue.Driver, options ...Option) http.Handler {
	if driver == nil {
		panic("dashboard: nil driver")
	}
	s := &server{driver: driver, config: config{overviewInterval: 2 * time.Second, jobsInterval: 1500 * time.Millisecond}}
	for _, option := range options {
		option(&s.config)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /app.js", s.asset("app.js", "application/javascript; charset=utf-8"))
	mux.HandleFunc("GET /style.css", s.asset("style.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /api/overview", s.endpoint(s.overviewHTTP))
	mux.HandleFunc("GET /api/stats", s.endpoint(s.statsHTTP))
	mux.HandleFunc("GET /api/queues", s.endpoint(s.queuesHTTP))
	mux.HandleFunc("GET /api/queues/{queue}", s.endpoint(s.queueHTTP))
	mux.HandleFunc("GET /api/jobs", s.endpoint(s.jobsHTTP))
	mux.HandleFunc("GET /api/jobs/{job_id}", s.endpoint(s.jobHTTP))
	mux.HandleFunc("POST /api/jobs/retry-failed", s.endpoint(s.retryFailedHTTP))
	mux.HandleFunc("POST /api/jobs/{job_id}/retry", s.endpoint(s.retryHTTP))
	mux.HandleFunc("POST /api/jobs/{job_id}/execute", s.endpoint(s.executeHTTP))
	mux.HandleFunc("DELETE /api/jobs/{job_id}", s.endpoint(s.deleteHTTP))
	mux.HandleFunc("GET /api/batches/{batch_id}", s.endpoint(s.batchHTTP))
	mux.HandleFunc("POST /api/prune", s.endpoint(s.pruneHTTP))
	mux.HandleFunc("GET /api/metrics", s.endpoint(s.metricsHTTP))
	mux.HandleFunc("GET /api/supervisors", s.endpoint(s.supervisorsHTTP))
	mux.HandleFunc("GET /ws", s.overviewWS)
	mux.HandleFunc("GET /ws/jobs", s.jobsWS)
	var handler http.Handler = mux
	if prefix := s.config.prefix; prefix != "" {
		stripped := http.StripPrefix(prefix, mux)
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == prefix {
				target := prefix + "/"
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				http.Redirect(w, r, target, http.StatusTemporaryRedirect)
				return
			}
			if !strings.HasPrefix(r.URL.Path, prefix+"/") {
				http.NotFound(w, r)
				return
			}
			stripped.ServeHTTP(w, r)
		})
	}
	if s.config.auth != nil {
		handler = s.config.auth(handler)
	}
	return handler
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	s.asset("index.html", "text/html; charset=utf-8")(w, r)
}

func (s *server) asset(name, contentType string) http.HandlerFunc {
	data, err := assets.ReadFile("static/" + name)
	if err != nil {
		panic(err)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
	}
}

type httpError struct {
	code    int
	message string
}

func (e *httpError) Error() string { return e.message }

func (s *server) endpoint(fn func(*http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := fn(r)
		code := http.StatusOK
		if err != nil {
			message := "Internal server error"
			code = http.StatusInternalServerError
			var apiErr *httpError
			switch {
			case errors.As(err, &apiErr):
				code, message = apiErr.code, apiErr.message
			case errors.Is(err, baqueue.ErrNotFound):
				code, message = http.StatusNotFound, "Job not found"
			case errors.Is(err, baqueue.ErrConflict):
				code, message = http.StatusConflict, "Job state changed"
			}
			data = map[string]any{"error": message}
		}
		body, err := json.Marshal(data)
		if err != nil {
			code = http.StatusInternalServerError
			body = []byte(`{"error":"Internal server error"}`)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(code)
		_, _ = w.Write(body)
	}
}
