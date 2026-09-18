package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/thushan/olla/internal/core/domain"
)

const (
	residencyCacheTTL = 2 * time.Second
	residencyTimeout  = 400 * time.Millisecond
	residencyMaxBody  = 1 << 20
)

type LoadedModel struct {
	Name          string    `json:"name"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	SizeVRAM      int64     `json:"size_vram,omitempty"`
	ContextLength int       `json:"context_length,omitempty"`
}

type ollamaResidencyResponse struct {
	Models []LoadedModel `json:"models"`
}
type residencyEntry struct {
	models    []LoadedModel
	expiresAt time.Time
}

type ollamaResidency struct {
	client *http.Client
	mu     sync.RWMutex
	cache  map[string]residencyEntry
}

func newOllamaResidency() *ollamaResidency {
	return &ollamaResidency{client: &http.Client{Timeout: residencyTimeout}, cache: make(map[string]residencyEntry)}
}

func (o *ollamaResidency) refresh(ctx context.Context, endpoints []*domain.Endpoint) map[string][]LoadedModel {
	result := make(map[string][]LoadedModel, len(endpoints))
	var wg sync.WaitGroup
	var resultMu sync.Mutex
	for _, endpoint := range endpoints {
		if endpoint == nil || (endpoint.Type != "" && !strings.EqualFold(endpoint.Type, "ollama")) {
			continue
		}
		key := endpoint.URLString
		o.mu.RLock()
		entry, cached := o.cache[key]
		o.mu.RUnlock()
		if cached && time.Now().Before(entry.expiresAt) {
			result[key] = entry.models
			continue
		}
		wg.Add(1)
		go func(endpoint *domain.Endpoint, key string) {
			defer wg.Done()
			models, err := o.probe(ctx, endpoint)
			if err != nil {
				return
			}
			o.mu.Lock()
			o.cache[key] = residencyEntry{models: models, expiresAt: time.Now().Add(residencyCacheTTL)}
			o.mu.Unlock()
			resultMu.Lock()
			result[key] = models
			resultMu.Unlock()
		}(endpoint, key)
	}
	wg.Wait()
	return result
}

func (o *ollamaResidency) probe(ctx context.Context, endpoint *domain.Endpoint) ([]LoadedModel, error) {
	if endpoint.URL == nil {
		return nil, errors.New("endpoint has no url")
	}
	pctx, cancel := context.WithTimeout(ctx, residencyTimeout)
	defer cancel()
	u := *endpoint.URL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/ps"
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	if endpoint.AuthHeaderName != "" && endpoint.AuthHeaderValue != "" {
		req.Header.Set(endpoint.AuthHeaderName, endpoint.AuthHeaderValue)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, errors.New("ollama /api/ps returned " + resp.Status)
	}
	var parsed ollamaResidencyResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, residencyMaxBody)).Decode(&parsed); err != nil {
		return nil, err
	}
	return parsed.Models, nil
}
