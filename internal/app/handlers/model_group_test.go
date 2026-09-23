package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

func TestResolveModelGroupUsesClassifierForPlainText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		_, _ = w.Write([]byte(`{"member":"fast","confidence":0.9}`))
	}))
	defer server.Close()

	app := &Application{Config: &config.Config{ModelGroups: map[string]config.ModelGroupConfig{
		"qwen3.5:auto": {
			FastModel: "qwen3.5:4b-mlx", CapableModel: "qwen3.5:9b-mlx",
			Classifier: config.ModelGroupClassifierConfig{URL: server.URL, Timeout: time.Second, MinConfidence: 0.8},
		},
	}}}
	body := `{"model":"qwen3.5:auto","messages":[{"role":"user","content":"Say hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	profile := domain.NewRequestProfile(req.URL.Path)
	profile.ModelName = "qwen3.5:auto"
	pr := &proxyRequest{model: "qwen3.5:auto", profile: profile, stats: &ports.RequestStats{}}

	app.resolveModelGroup(context.Background(), req, pr)

	if pr.model != "qwen3.5:4b-mlx" || pr.modelGroupSource != "classifier" {
		t.Fatalf("unexpected resolution: model=%q source=%q", pr.model, pr.modelGroupSource)
	}
	if profile.ModelName != pr.model || pr.stats.Model != pr.model {
		t.Fatalf("model group resolution did not update request metadata")
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil || !strings.Contains(string(restored), `"model":"qwen3.5:4b-mlx"`) {
		t.Fatalf("request body was not restored: body=%q err=%v", restored, err)
	}
}

func TestResolveModelGroupUsesCapableForToolRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"member":"fast","confidence":1}`))
	}))
	defer server.Close()

	app := &Application{Config: &config.Config{ModelGroups: map[string]config.ModelGroupConfig{
		"qwen3.5:auto": {
			FastModel: "qwen3.5:4b-mlx", CapableModel: "qwen3.5:9b-mlx",
			Classifier: config.ModelGroupClassifierConfig{URL: server.URL},
		},
	}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen3.5:auto","tools":[{}],"messages":[{"content":"Use a tool"}]}`))
	profile := domain.NewRequestProfile(req.URL.Path)
	profile.ModelName = "qwen3.5:auto"
	profile.RequiresFunctionCall = true
	pr := &proxyRequest{model: "qwen3.5:auto", profile: profile, stats: &ports.RequestStats{}}

	app.resolveModelGroup(context.Background(), req, pr)

	if called || pr.model != "qwen3.5:9b-mlx" || pr.modelGroupSource != "fallback" {
		t.Fatalf("tool request must bypass classifier, got called=%t model=%q source=%q", called, pr.model, pr.modelGroupSource)
	}
}
