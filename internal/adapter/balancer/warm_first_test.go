package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

const sharedModel = "llama3.2:3b"
const largeModel = "llama3.1:70b"

func withModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, constants.ContextModelKey, model)
}

func withModelAndCtx(ctx context.Context, model string, numCtx int) context.Context {
	ctx = withModel(ctx, model)
	return context.WithValue(ctx, constants.ContextNumCtxKey, numCtx)
}

func newWarmFirstWithLookup(lookup psLookup) *WarmFirstSelector {
	s := NewWarmFirstSelector(NewTestStatsCollector())
	s.lookup = lookup
	return s
}

func endpointNamed(name string, port int, prio int) *domain.Endpoint {
	ep := createTestEndpoint(name, port, domain.StatusHealthy)
	ep.Priority = prio
	ep.Type = "ollama"
	return ep
}

func TestWarmFirstSelector_Name(t *testing.T) {
	s := NewWarmFirstSelector(NewTestStatsCollector())
	if s.Name() != DefaultBalancerWarmFirst {
		t.Errorf("Name() = %q", s.Name())
	}
}

func TestWarmFirstSelector_Select_NoEndpoints(t *testing.T) {
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	ep, err := s.Select(context.Background(), nil)
	if err == nil || ep != nil {
		t.Fatalf("expected error, got %v %v", ep, err)
	}
}

func TestWarmFirstSelector_Select_IdleWarmBeatsIdleFree(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{sharedModel}, nil
		}
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (idle+warm)", got.Name)
	}
}

func TestWarmFirstSelector_Select_IdleFreeBeatsBusyWarm(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{sharedModel}, nil
		}
		return nil, nil
	})
	s.IncrementConnections(a)
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b (idle endpoint beats queue on a warm busy node)", got.Name)
	}
}

func TestWarmFirstSelector_Select_IdleFreeBeatsIdleOther(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{largeModel}, nil
		}
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b (do not evict a different loaded model)", got.Name)
	}
}

func TestWarmFirstSelector_Select_BusyWarmBeatsIdleOther(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{largeModel}, nil
		}
		return []string{sharedModel}, nil
	})
	s.IncrementConnections(b)
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b (busy+warm over evicting another model)", got.Name)
	}
}

func TestWarmFirstSelector_Select_SingleCompatibleEndpoint(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), largeModel), []*domain.Endpoint{a})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a", got.Name)
	}
}

func TestWarmFirstSelector_Select_ProbeErrorFailOpen(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return nil, errors.New("ps down")
		}
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b (successful free probe beats unknown)", got.Name)
	}
}

func TestWarmFirstSelector_Select_NoModelIdleBeatsBusy(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		t.Fatal("lookup should not run without a model")
		return nil, nil
	})
	s.IncrementConnections(a)
	got, err := s.Select(context.Background(), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b", got.Name)
	}
}

func TestWarmFirstSelector_Select_PriorityTieBreak(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{b, a})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (same rank, higher priority)", got.Name)
	}
}

func TestWarmFirstSelector_ProbeOllamaPS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ps" {
			t.Errorf("path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"models": []map[string]string{
				{"name": sharedModel, "model": sharedModel},
			},
		})
	}))
	t.Cleanup(srv.Close)

	u, _ := url.Parse(srv.URL)
	ep := &domain.Endpoint{
		Name:      "node-a",
		URL:       u,
		URLString: u.String(),
		Type:      "ollama",
		Status:    domain.StatusHealthy,
		Priority:  100,
	}
	s := NewWarmFirstSelector(NewTestStatsCollector())
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{ep})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s", got.Name)
	}
	if _, err := s.cachedLookup(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
}

func TestWarmFirstSelector_ProbeErrorHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	u, _ := url.Parse(srv.URL)
	ep := &domain.Endpoint{
		Name:      "node-a",
		URL:       u,
		URLString: u.String(),
		Type:      "ollama",
		Status:    domain.StatusHealthy,
	}
	s := NewWarmFirstSelector(NewTestStatsCollector())
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{ep})
	if err != nil || got == nil {
		t.Fatalf("fail-open expected, got %v %v", got, err)
	}
}

func TestWarmFirstSelector_CacheTTL(t *testing.T) {
	calls := 0
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		calls++
		return []string{sharedModel}, nil
	})
	ep := endpointNamed("node-a", 11434, 100)
	ctx := withModel(context.Background(), sharedModel)
	_, _ = s.Select(ctx, []*domain.Endpoint{ep})
	_, _ = s.Select(ctx, []*domain.Endpoint{ep})
	if calls != 1 {
		t.Errorf("lookup calls = %d, want 1 (cached)", calls)
	}
	s.mu.Lock()
	entry := s.cache[ep.URLString]
	entry.expiresAt = time.Now().Add(-time.Second)
	s.cache[ep.URLString] = entry
	s.mu.Unlock()
	_, _ = s.Select(ctx, []*domain.Endpoint{ep})
	if calls != 2 {
		t.Errorf("lookup calls = %d, want 2 after TTL", calls)
	}
}

func TestWarmFirstSelector_Select_NumParallelKeepsWarmSlot(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.NumParallel = 2
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{sharedModel}, nil
		}
		return nil, nil
	})
	s.IncrementConnections(a)
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (one in-flight of two parallel slots is still idle+warm)", got.Name)
	}
}

func TestWarmFirstSelector_Select_RoomUnderMaxLoadedIsFree(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.MaxLoadedModels = 2
	b := endpointNamed("node-b", 11435, 50)
	s := newWarmFirstWithLookup(func(_ context.Context, ep *domain.Endpoint) ([]string, error) {
		if ep.Name == "node-a" {
			return []string{largeModel}, nil
		}
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (second slot free, higher priority)", got.Name)
	}
}

func TestWarmFirstSelector_Select_SmallestContextWinsSameRank(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.ContextLength = 262144
	b := endpointNamed("node-b", 11435, 50)
	b.ContextLength = 32768
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-b" {
		t.Errorf("got %s, want node-b (pack onto the smaller context window)", got.Name)
	}
}

func TestWarmFirstSelector_Select_PriorityTiebreakIgnoresSmallerWindow(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.ContextLength = 262144
	b := endpointNamed("node-b", 11435, 50)
	b.ContextLength = 32768
	s := NewWarmFirstSelectorWithTiebreak(NewTestStatsCollector(), ContextTiebreakPriority)
	s.lookup = func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	}
	got, err := s.Select(withModel(context.Background(), sharedModel), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (priority tie-break, not smallest window)", got.Name)
	}
}

func TestWarmFirstSelector_Select_SkipTooSmallContext(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.ContextLength = 262144
	b := endpointNamed("node-b", 11435, 50)
	b.ContextLength = 32768
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	got, err := s.Select(withModelAndCtx(context.Background(), sharedModel, 200000), []*domain.Endpoint{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (node-b's 32k window cannot fit 200k)", got.Name)
	}
}

func TestWarmFirstSelector_Select_ContextFilterFailOpen(t *testing.T) {
	a := endpointNamed("node-a", 11434, 100)
	a.ContextLength = 32768
	s := newWarmFirstWithLookup(func(context.Context, *domain.Endpoint) ([]string, error) {
		return nil, nil
	})
	got, err := s.Select(withModelAndCtx(context.Background(), sharedModel, 200000), []*domain.Endpoint{a})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "node-a" {
		t.Errorf("got %s, want node-a (only candidate, even if window is small)", got.Name)
	}
}
