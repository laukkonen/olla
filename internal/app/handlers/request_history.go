package handlers

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/thushan/olla/internal/app/middleware"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/util"
)

const requestHistoryCapacity = 200

// RequestRecord is deliberately an in-memory operational view. It is never
// written to logs, disk, metrics labels, or a persistent store.
type RequestRecord struct {
	Timestamp       time.Time `json:"timestamp"`
	RequestID       string    `json:"request_id"`
	ClientIP        string    `json:"client_ip,omitempty"`
	RemoteAddr      string    `json:"remote_addr,omitempty"`
	UserAgent       string    `json:"user_agent,omitempty"`
	Method          string    `json:"method"`
	Path            string    `json:"path"`
	Model           string    `json:"model,omitempty"`
	Endpoint        string    `json:"endpoint,omitempty"`
	Backend         string    `json:"backend,omitempty"`
	Status          int       `json:"status"`
	DurationMs      int64     `json:"duration_ms"`
	RequestBytes    int64     `json:"request_bytes"`
	ResponseBytes   int64     `json:"response_bytes"`
	RoutingStrategy string    `json:"routing_strategy,omitempty"`
	RoutingDecision string    `json:"routing_decision,omitempty"`
	RoutingReason   string    `json:"routing_reason,omitempty"`
	AdmissionClass  string    `json:"admission_class,omitempty"`
	AdmissionSource string    `json:"admission_source,omitempty"`
	StickySession   string    `json:"sticky_session,omitempty"`
	StickyKeySource string    `json:"sticky_key_source,omitempty"`
	State           string    `json:"state"`
}

type requestHistory struct {
	mu    sync.RWMutex
	items [requestHistoryCapacity]RequestRecord
	start int
	count int
}

func newRequestHistory() *requestHistory { return &requestHistory{} }

func (h *requestHistory) begin(r *http.Request, cfg config.Config) *RequestRecord {
	if !middleware.IsProxyRequest(r.URL.Path) {
		return nil
	}
	id := middleware.GetRequestID(r.Context())
	if id == "" {
		id = r.Header.Get(constants.HeaderXRequestID)
	}
	clientIP := util.GetClientIP(r, cfg.Server.RateLimits.TrustProxyHeaders, cfg.Server.RateLimits.TrustedProxyCIDRsParsed)
	record := &RequestRecord{
		Timestamp: time.Now(), RequestID: id, ClientIP: clientIP, RemoteAddr: r.RemoteAddr,
		UserAgent: r.UserAgent(), Method: r.Method, Path: r.URL.Path,
		RequestBytes: maxZero(r.ContentLength), Status: http.StatusOK, State: "active",
	}
	return record
}

func (h *requestHistory) finish(record *RequestRecord, responseHeaders http.Header, status int, responseBytes int64, started time.Time) {
	if record == nil {
		return
	}
	record.Status = status
	record.ResponseBytes = responseBytes
	record.DurationMs = time.Since(started).Milliseconds()
	record.State = "completed"
	record.Model = responseHeaders.Get(constants.HeaderXOllaModel)
	record.Endpoint = responseHeaders.Get(constants.HeaderXOllaEndpoint)
	record.Backend = responseHeaders.Get(constants.HeaderXOllaBackendType)
	record.RoutingStrategy = responseHeaders.Get(constants.HeaderXOllaRoutingStrategy)
	record.RoutingDecision = responseHeaders.Get(constants.HeaderXOllaRoutingDecision)
	record.RoutingReason = responseHeaders.Get(constants.HeaderXOllaRoutingReason)
	record.AdmissionClass = responseHeaders.Get(constants.HeaderXOllaClass)
	record.AdmissionSource = responseHeaders.Get(constants.HeaderXOllaClassSource)
	record.StickySession = responseHeaders.Get(constants.HeaderXOllaStickySession)
	record.StickyKeySource = responseHeaders.Get(constants.HeaderXOllaStickyKeySource)
	h.mu.Lock()
	if h.count < requestHistoryCapacity {
		h.items[(h.start+h.count)%requestHistoryCapacity] = *record
		h.count++
	} else {
		h.items[h.start] = *record
		h.start = (h.start + 1) % requestHistoryCapacity
	}
	h.mu.Unlock()
}

func (h *requestHistory) snapshot() []RequestRecord {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]RequestRecord, h.count)
	for i := range out {
		out[i] = h.items[(h.start+i)%requestHistoryCapacity]
	}
	return out
}

func (h *requestHistory) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(struct {
		Requests []RequestRecord `json:"requests"`
		Capacity int             `json:"capacity"`
	}{h.snapshot(), requestHistoryCapacity})
}

func maxZero(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

type historyResponseWriter struct {
	http.ResponseWriter
	status int
	size   int64
}

func (w *historyResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *historyResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.size += int64(n)
	return n, err
}
func (w *historyResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (w *historyResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (a *Application) requestHistoryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		record := a.requestHistory.begin(r, *a.Config)
		wrapped := &historyResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		a.requestHistory.finish(record, wrapped.Header(), wrapped.status, wrapped.size, started)
	})
}
