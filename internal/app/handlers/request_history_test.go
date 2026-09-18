package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thushan/olla/internal/config"
)

func TestRequestHistoryIsBoundedAndOrdered(t *testing.T) {
	h := newRequestHistory()
	cfg := config.Config{}
	for i := 0; i < requestHistoryCapacity+3; i++ {
		r := httptest.NewRequest(http.MethodPost, "/olla/proxy/v1/chat", nil)
		r.RemoteAddr = "192.0.2.1:1234"
		r.Header.Set("X-Request-ID", string(rune(i+33)))
		record := h.begin(r, cfg)
		h.finish(record, http.Header{"X-Olla-Endpoint": []string{"node"}}, http.StatusOK, 10, time.Now())
	}
	items := h.snapshot()
	if len(items) != requestHistoryCapacity {
		t.Fatalf("history length = %d, want %d", len(items), requestHistoryCapacity)
	}
	if items[0].RequestID != string(rune(36)) || items[len(items)-1].RequestID != string(rune(requestHistoryCapacity+35)) {
		t.Fatalf("ring did not retain newest records: first=%q last=%q", items[0].RequestID, items[len(items)-1].RequestID)
	}
}

func TestRequestHistoryIgnoresNonProxyRequests(t *testing.T) {
	h := newRequestHistory()
	r := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	if record := h.begin(r, config.Config{}); record != nil {
		t.Fatal("non-proxy request should not be recorded")
	}
}
