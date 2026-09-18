package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/core/domain"
)

func TestOllamaResidencyRefresh(t *testing.T) {
	expires := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ps" {
			t.Errorf("path = %q, want /api/ps", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"models":[{"name":"qwen3.8:27b-mxfp8","expires_at":"2026-09-13T12:00:00Z","size_vram":1234,"context_length":32768}]}`))
	}))
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)

	tracker := newOllamaResidency()
	loaded := tracker.refresh(context.Background(), []*domain.Endpoint{{
		Type: "ollama", URL: parsed, URLString: server.URL,
	}})
	require.Len(t, loaded[server.URL], 1)
	require.Equal(t, "qwen3.8:27b-mxfp8", loaded[server.URL][0].Name)
	require.Equal(t, expires, loaded[server.URL][0].ExpiresAt)
	require.Equal(t, int64(1234), loaded[server.URL][0].SizeVRAM)
	require.Equal(t, 32768, loaded[server.URL][0].ContextLength)
}
