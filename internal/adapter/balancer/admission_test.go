package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

func defaultAdmissionConfig() config.AdmissionConfig {
	return config.AdmissionConfig{
		Enabled:      true,
		DefaultClass: "batch",
		Header:       constants.HeaderXOllaClass,
		Classes: map[string]config.AdmissionClass{
			"interactive": {Weight: 10},
			"batch":       {Weight: 1},
		},
		CIDRs: []config.AdmissionCIDR{
			{CIDR: "127.0.0.1/32", Class: "interactive"},
			{CIDR: "192.168.1.212/32", Class: "batch"},
		},
	}
}

type stubSelector struct {
	calls atomic.Int32
}

func (s *stubSelector) Name() string { return "stub" }
func (s *stubSelector) Select(_ context.Context, endpoints []*domain.Endpoint) (*domain.Endpoint, error) {
	s.calls.Add(1)
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints")
	}
	return endpoints[0], nil
}
func (s *stubSelector) IncrementConnections(*domain.Endpoint) {}
func (s *stubSelector) DecrementConnections(*domain.Endpoint) {}

func injectAdmission(ctx context.Context, ip, header string) (context.Context, *domain.AdmissionOutcome) {
	outcome := &domain.AdmissionOutcome{}
	ctx = context.WithValue(ctx, constants.ContextClientIPKey, ip)
	ctx = context.WithValue(ctx, constants.ContextAdmissionHeaderKey, header)
	ctx = context.WithValue(ctx, constants.ContextAdmissionOutcomeKey, outcome)
	return ctx, outcome
}

func TestResolveAdmissionClass(t *testing.T) {
	t.Parallel()
	cfg := defaultAdmissionConfig()

	class, source, weight := ResolveAdmissionClass("192.168.1.212", "interactive", cfg)
	assert.Equal(t, "interactive", class)
	assert.Equal(t, admissionSourceHeader, source)
	assert.Equal(t, 10, weight)

	class, source, weight = ResolveAdmissionClass("127.0.0.1", "", cfg)
	assert.Equal(t, "interactive", class)
	assert.Equal(t, admissionSourceCIDR, source)
	assert.Equal(t, 10, weight)

	class, source, weight = ResolveAdmissionClass("192.168.1.212", "", cfg)
	assert.Equal(t, "batch", class)
	assert.Equal(t, admissionSourceCIDR, source)
	assert.Equal(t, 1, weight)

	class, source, weight = ResolveAdmissionClass("10.0.0.1", "nope", cfg)
	assert.Equal(t, "batch", class)
	assert.Equal(t, admissionSourceDefault, source)
	assert.Equal(t, 1, weight)
}

func TestAdmissionWrapper_IdleSelectsImmediately(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1

	ctx, outcome := injectAdmission(context.Background(), "10.0.0.1", "")
	chosen, err := w.Select(ctx, []*domain.Endpoint{ep})
	require.NoError(t, err)
	assert.Equal(t, ep, chosen)
	assert.Equal(t, int32(1), inner.calls.Load())
	assert.Equal(t, "batch", outcome.Class)
	assert.Equal(t, admissionSourceDefault, outcome.Source)
	assert.Equal(t, time.Duration(0), outcome.Waited)
}

func TestAdmissionWrapper_WaitsUntilDecrement(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	ctx, outcome := injectAdmission(context.Background(), "10.0.0.1", "batch")

	done := make(chan struct{})
	var chosen *domain.Endpoint
	var err error
	go func() {
		chosen, err = w.Select(ctx, []*domain.Endpoint{ep})
		close(done)
	}()

	require.Eventually(t, func() bool { return w.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	select {
	case <-done:
		t.Fatal("Select returned before a slot was freed")
	default:
	}

	w.DecrementConnections(ep)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Select did not return after DecrementConnections")
	}
	require.NoError(t, err)
	assert.Equal(t, ep, chosen)
	assert.Greater(t, outcome.Waited, time.Duration(0))
	assert.Equal(t, "batch", outcome.Class)
}

func TestAdmissionWrapper_InteractiveJumpsBatch(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	batchCtx, _ := injectAdmission(context.Background(), "10.0.0.1", "batch")
	interactiveCtx, _ := injectAdmission(context.Background(), "10.0.0.1", "interactive")

	var order []string
	var mu sync.Mutex
	record := func(class string) {
		mu.Lock()
		order = append(order, class)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := w.Select(batchCtx, []*domain.Endpoint{ep})
		require.NoError(t, err)
		record("batch")
	}()
	require.Eventually(t, func() bool { return w.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	go func() {
		defer wg.Done()
		_, err := w.Select(interactiveCtx, []*domain.Endpoint{ep})
		require.NoError(t, err)
		record("interactive")
	}()
	require.Eventually(t, func() bool { return w.waiterCount() == 2 }, time.Second, 5*time.Millisecond)

	w.DecrementConnections(ep)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 1
	}, time.Second, 5*time.Millisecond)

	mu.Lock()
	first := order[0]
	mu.Unlock()
	assert.Equal(t, "interactive", first, "interactive waiter should be granted first")

	w.DecrementConnections(ep)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"interactive", "batch"}, order)
}

func TestAdmissionWrapper_TimeoutFallsThrough(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	cfg := defaultAdmissionConfig()
	cfg.WaitTimeout = 30 * time.Millisecond
	w := NewAdmissionWrapper(inner, stats, cfg)
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	ctx, outcome := injectAdmission(context.Background(), "10.0.0.1", "batch")
	chosen, err := w.Select(ctx, []*domain.Endpoint{ep})
	require.NoError(t, err)
	assert.Equal(t, ep, chosen)
	assert.Equal(t, int32(1), inner.calls.Load())
	assert.GreaterOrEqual(t, outcome.Waited, 20*time.Millisecond)
}

func TestAdmissionWrapper_CancelUnblocks(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	ctx, cancel := context.WithCancel(context.Background())
	ctx, _ = injectAdmission(ctx, "10.0.0.1", "batch")

	errCh := make(chan error, 1)
	go func() {
		_, err := w.Select(ctx, []*domain.Endpoint{ep})
		errCh <- err
	}()
	require.Eventually(t, func() bool { return w.waiterCount() == 1 }, time.Second, 5*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, int32(0), inner.calls.Load())
	case <-time.After(2 * time.Second):
		t.Fatal("Select did not return on cancel")
	}
}

func TestAdmissionWrapper_QueueSnapshotCarriesSafeMetadata(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx = context.WithValue(ctx, constants.ContextModelKey, "llama3:8b")
	ctx = context.WithValue(ctx, constants.ContextRequestIdKey, "req-123")
	ctx = context.WithValue(ctx, constants.ContextNumCtxKey, 8192)
	ctx, _ = injectAdmission(ctx, "192.0.2.10", "interactive")
	go func() { _, _ = w.Select(ctx, []*domain.Endpoint{ep}) }()
	require.Eventually(t, func() bool { return w.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	snapshot := w.QueueSnapshot()
	require.Len(t, snapshot.Items, 1)
	assert.Equal(t, 1, snapshot.Waiting)
	assert.Equal(t, 1, snapshot.ByClass["interactive"])
	assert.Equal(t, 1, snapshot.ByModel["llama3:8b"])
	item := snapshot.Items[0]
	assert.Equal(t, 1, item.Position)
	assert.Equal(t, "req-123", item.RequestID)
	assert.Equal(t, 8192, item.RequestedContext)
	assert.NotNil(t, item.Deadline)
	assert.NotContains(t, string(mustJSON(t, snapshot)), "192.0.2.10")
	cancel()
}

func mustJSON(t *testing.T, value interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	require.NoError(t, err)
	return b
}

func TestAdmissionWrapper_Name(t *testing.T) {
	t.Parallel()
	w := NewAdmissionWrapper(&stubSelector{}, NewTestStatsCollector(), defaultAdmissionConfig())
	assert.Equal(t, "admission(stub)", w.Name())
}

func TestAdmissionWrapper_HigherWeightTakesIdleWhileLowerWaits(t *testing.T) {
	t.Parallel()

	inner := &stubSelector{}
	stats := NewTestStatsCollector()
	w := NewAdmissionWrapper(inner, stats, defaultAdmissionConfig())
	ep := makeEndpoint("gpu", "http://127.0.0.1:11434")
	ep.NumParallel = 1
	stats.RecordConnection(ep, 1)

	batchCtx, cancelBatch := context.WithCancel(context.Background())
	t.Cleanup(cancelBatch)
	batchCtx, _ = injectAdmission(batchCtx, "10.0.0.1", "batch")
	go func() {
		_, _ = w.Select(batchCtx, []*domain.Endpoint{ep})
	}()
	require.Eventually(t, func() bool { return w.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	// Slot frees; interactive arrives in the race window before the waiter runs.
	// Connection count is still 1 here (stub Increment/Decrement don't touch stats),
	// so drop it to simulate the real Decrement path, then the interactive request
	// should take the idle slot because its weight beats the waiting batch job.
	stats.RecordConnection(ep, -1)

	interactiveCtx, _ := injectAdmission(context.Background(), "10.0.0.1", "interactive")
	chosen, err := w.Select(interactiveCtx, []*domain.Endpoint{ep})
	require.NoError(t, err)
	assert.Equal(t, ep, chosen)
	assert.Equal(t, int32(1), inner.calls.Load(), "interactive should skip the queue")
}
