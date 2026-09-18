package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

const (
	warmFirstPSPath       = "/api/ps"
	warmFirstCacheTTL     = 2 * time.Second
	warmFirstProbeTimeout = 400 * time.Millisecond
	warmFirstMaxBody      = 1 << 20

	// Defaults when endpoint YAML omits the Ollama capacity fields.
	warmFirstDefaultMaxLoaded = 1
	warmFirstDefaultParallel  = 1

	rankIdleWarm = iota
	rankIdleFree
	rankBusyWarm
	rankOther

	// ContextTiebreakSmallest packs same-rank jobs onto the smallest
	// context_length that still fits (default). ContextTiebreakPriority uses
	// YAML priority instead so a 256k node is not starved of default-ctx 27B.
	ContextTiebreakSmallest = "smallest"
	ContextTiebreakPriority = "priority"
)

// ollamaPSResponse is the subset of GET /api/ps used for model residency.
type ollamaPSResponse struct {
	Models []struct {
		Name  string `json:"name"`
		Model string `json:"model"`
	} `json:"models"`
}

type psCacheEntry struct {
	models    []string
	err       error
	expiresAt time.Time
}

// psLookup returns loaded model names, or an error if the probe failed.
// An empty slice and a nil error means nothing is loaded.
type psLookup func(ctx context.Context, endpoint *domain.Endpoint) ([]string, error)

// WarmFirstSelector prefers an idle endpoint that already has the requested
// model resident, then an idle endpoint that can load it without eviction,
// then a busy endpoint that already has the model, and only then an endpoint
// that would have to evict a different model. In-flight counts come from Olla;
// residency comes from Ollama GET /api/ps (Ollama endpoints only).
//
// Capacity comes from optional endpoint fields that mirror Ollama env:
// MaxLoadedModels, NumParallel, ContextLength. Zero means the defaults above.
// Among equal rank, default is smallest context_length that still fits
// (context_tiebreak: smallest). context_tiebreak: priority skips packing.
//
// Other endpoint types are ranked last (probe skipped) so they remain eligible
// without affecting Ollama placement.
type WarmFirstSelector struct {
	statsCollector  ports.StatsCollector
	client          *http.Client
	lookup          psLookup
	cache           map[string]psCacheEntry
	mu              sync.Mutex
	cursor          atomic.Uint64
	contextTiebreak string
}

func NewWarmFirstSelector(statsCollector ports.StatsCollector) *WarmFirstSelector {
	return NewWarmFirstSelectorWithTiebreak(statsCollector, ContextTiebreakSmallest)
}

func NewWarmFirstSelectorWithTiebreak(statsCollector ports.StatsCollector, contextTiebreak string) *WarmFirstSelector {
	tb := strings.ToLower(strings.TrimSpace(contextTiebreak))
	if tb != ContextTiebreakPriority {
		tb = ContextTiebreakSmallest
	}
	s := &WarmFirstSelector{
		statsCollector:  statsCollector,
		contextTiebreak: tb,
		client: &http.Client{
			Timeout: warmFirstProbeTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		cache: make(map[string]psCacheEntry),
	}
	s.lookup = s.probeOllamaPS
	return s
}

func (w *WarmFirstSelector) Name() string {
	return DefaultBalancerWarmFirst
}

func (w *WarmFirstSelector) IncrementConnections(endpoint *domain.Endpoint) {
	w.statsCollector.RecordConnection(endpoint, 1)
}

func (w *WarmFirstSelector) DecrementConnections(endpoint *domain.Endpoint) {
	w.statsCollector.RecordConnection(endpoint, -1)
}

func (w *WarmFirstSelector) Select(ctx context.Context, endpoints []*domain.Endpoint) (*domain.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints available")
	}

	routable := make([]*domain.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Status.IsRoutable() {
			routable = append(routable, endpoint)
		}
	}
	if len(routable) == 0 {
		return nil, errors.New("no routable endpoints available")
	}

	model, _ := ctx.Value(constants.ContextModelKey).(string)
	numCtx, _ := ctx.Value(constants.ContextNumCtxKey).(int)
	routable = filterByRequestedContext(routable, numCtx)

	bestRank := rankOther + 1
	tied := make([]*domain.Endpoint, 0, len(routable))
	minConns := int64(-1)

	for _, endpoint := range routable {
		rank := w.rank(ctx, endpoint, model)
		conns := w.statsCollector.GetConnectionCount(endpoint.URLString)
		switch {
		case rank < bestRank:
			bestRank = rank
			minConns = conns
			tied = tied[:0]
			tied = append(tied, endpoint)
		case rank == bestRank && (minConns == -1 || conns < minConns):
			minConns = conns
			tied = tied[:0]
			tied = append(tied, endpoint)
		case rank == bestRank && conns == minConns:
			tied = append(tied, endpoint)
		}
	}

	return w.pickTied(tied), nil
}

// pickTied prefers the smallest configured context window (default), then
// higher priority, then rotates. With context_tiebreak: priority, packing is
// skipped so the higher-priority node wins when both are idle. Endpoints with
// ContextLength 0 (unknown) sort after any known window when packing.
func (w *WarmFirstSelector) pickTied(tied []*domain.Endpoint) *domain.Endpoint {
	if len(tied) == 1 {
		return tied[0]
	}

	packed := tied
	if w.contextTiebreak != ContextTiebreakPriority {
		packed = preferSmallestContext(tied)
		if len(packed) == 1 {
			return packed[0]
		}
	}

	bestPrio := packed[0].Priority
	prioTied := make([]*domain.Endpoint, 0, len(packed))
	for _, endpoint := range packed {
		if endpoint.Priority > bestPrio {
			bestPrio = endpoint.Priority
			prioTied = prioTied[:0]
			prioTied = append(prioTied, endpoint)
		} else if endpoint.Priority == bestPrio {
			prioTied = append(prioTied, endpoint)
		}
	}
	if len(prioTied) == 1 {
		return prioTied[0]
	}
	idx := w.cursor.Add(1) - 1
	return prioTied[idx%uint64(len(prioTied))]
}

func (w *WarmFirstSelector) rank(ctx context.Context, endpoint *domain.Endpoint, model string) int {
	idle := w.statsCollector.GetConnectionCount(endpoint.URLString) < int64(numParallelSlots(endpoint))
	if model == "" {
		if idle {
			return rankIdleFree
		}
		return rankBusyWarm
	}

	loaded, err := w.cachedLookup(ctx, endpoint)
	if err != nil {
		return rankOther
	}

	switch {
	case modelsContain(loaded, model):
		if idle {
			return rankIdleWarm
		}
		return rankBusyWarm
	case uniqueLoadedCount(loaded) < maxLoadedModels(endpoint):
		if idle {
			return rankIdleFree
		}
		return rankOther
	default:
		return rankOther
	}
}

func (w *WarmFirstSelector) cachedLookup(ctx context.Context, endpoint *domain.Endpoint) ([]string, error) {
	key := endpoint.URLString
	now := time.Now()

	w.mu.Lock()
	if entry, ok := w.cache[key]; ok && now.Before(entry.expiresAt) {
		w.mu.Unlock()
		return entry.models, entry.err
	}
	w.mu.Unlock()

	models, err := w.lookup(ctx, endpoint)

	w.mu.Lock()
	w.cache[key] = psCacheEntry{models: models, err: err, expiresAt: now.Add(warmFirstCacheTTL)}
	w.mu.Unlock()
	return models, err
}

func (w *WarmFirstSelector) probeOllamaPS(ctx context.Context, endpoint *domain.Endpoint) ([]string, error) {
	if endpoint == nil || endpoint.URL == nil {
		return nil, errors.New("endpoint has no url")
	}
	kind := strings.ToLower(endpoint.Type)
	if kind != "" && kind != "ollama" {
		return nil, errors.New("warm-first /api/ps is ollama-only")
	}

	pctx, cancel := context.WithTimeout(ctx, warmFirstProbeTimeout)
	defer cancel()

	u := *endpoint.URL
	u.Path = strings.TrimSuffix(u.Path, "/") + warmFirstPSPath
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	if endpoint.AuthHeaderName != "" && endpoint.AuthHeaderValue != "" {
		req.Header.Set(endpoint.AuthHeaderName, endpoint.AuthHeaderValue)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, errors.New("ollama /api/ps returned " + resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, warmFirstMaxBody))
	if err != nil {
		return nil, err
	}
	var parsed ollamaPSResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(parsed.Models)*2)
	seen := make(map[string]struct{}, len(parsed.Models)*2)
	for _, m := range parsed.Models {
		for _, n := range []string{m.Name, m.Model} {
			if n == "" {
				continue
			}
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}
	return names, nil
}

func modelsContain(loaded []string, want string) bool {
	if want == "" {
		return false
	}
	for _, n := range loaded {
		if n == want || strings.EqualFold(n, want) {
			return true
		}
	}
	return false
}

func uniqueLoadedCount(loaded []string) int {
	if len(loaded) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, len(loaded))
	for _, n := range loaded {
		seen[strings.ToLower(n)] = struct{}{}
	}
	return len(seen)
}

func maxLoadedModels(endpoint *domain.Endpoint) int {
	if endpoint == nil || endpoint.MaxLoadedModels <= 0 {
		return warmFirstDefaultMaxLoaded
	}
	return endpoint.MaxLoadedModels
}

func numParallelSlots(endpoint *domain.Endpoint) int {
	if endpoint == nil || endpoint.NumParallel <= 0 {
		return warmFirstDefaultParallel
	}
	return endpoint.NumParallel
}

// filterByRequestedContext drops endpoints whose declared window is smaller
// than the request. Unknown windows (ContextLength 0) always pass. If nothing
// would remain, the original list is kept (fail-open).
func filterByRequestedContext(endpoints []*domain.Endpoint, numCtx int) []*domain.Endpoint {
	if numCtx <= 0 || len(endpoints) == 0 {
		return endpoints
	}
	fit := make([]*domain.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.ContextLength <= 0 || endpoint.ContextLength >= numCtx {
			fit = append(fit, endpoint)
		}
	}
	if len(fit) == 0 {
		return endpoints
	}
	return fit
}

func preferSmallestContext(tied []*domain.Endpoint) []*domain.Endpoint {
	best := 0
	known := make([]*domain.Endpoint, 0, len(tied))
	for _, endpoint := range tied {
		if endpoint.ContextLength <= 0 {
			continue
		}
		if best == 0 || endpoint.ContextLength < best {
			best = endpoint.ContextLength
			known = known[:0]
			known = append(known, endpoint)
		} else if endpoint.ContextLength == best {
			known = append(known, endpoint)
		}
	}
	if len(known) == 0 {
		return tied
	}
	return known
}
