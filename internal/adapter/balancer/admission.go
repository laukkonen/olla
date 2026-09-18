package balancer

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

const (
	admissionSourceHeader  = "header"
	admissionSourceCIDR    = "cidr"
	admissionSourceDefault = "default"
)

type admissionWaiter struct {
	ctx              context.Context
	ready            chan struct{}
	class            string
	id               uint64
	seq              uint64
	weight           int
	model            string
	requestID        string
	requestedContext int
	queuedAt         time.Time
	deadline         time.Time
	hasDeadline      bool
	waitTimeout      time.Duration
}

// QueueSnapshot is a read-only view of requests currently waiting for admission.
// It deliberately contains no client address or other network identity.
type QueueSnapshot struct {
	Waiting      int            `json:"waiting"`
	OldestWaitMs int64          `json:"oldest_wait_ms"`
	ByClass      map[string]int `json:"by_class"`
	ByModel      map[string]int `json:"by_model"`
	Items        []QueueItem    `json:"items"`
}

type QueueItem struct {
	Position         int        `json:"position"`
	RequestID        string     `json:"request_id,omitempty"`
	Model            string     `json:"model,omitempty"`
	Class            string     `json:"class"`
	QueuedAt         time.Time  `json:"queued_at"`
	WaitedMs         int64      `json:"waited_ms"`
	RequestedContext int        `json:"requested_context,omitempty"`
	Deadline         *time.Time `json:"deadline,omitempty"`
	TimeoutMs        int64      `json:"timeout_ms,omitempty"`
}

// AdmissionWrapper is a decorator around any EndpointSelector that blocks
// until some eligible endpoint has an in-flight count below num_parallel, or
// until wait_timeout / request cancel. Higher-weight classes (interactive)
// jump the queue; equal weight is FIFO. Sticky sessions must wrap *outside*
// this so a KV-cache hit never waits.
//
// This does not add GPU capacity. It only decides who runs when every worker
// is already generating.
type AdmissionWrapper struct {
	inner   domain.EndpointSelector
	stats   ports.StatsCollector
	cfg     config.AdmissionConfig
	mu      sync.Mutex
	waiters []*admissionWaiter
	seq     atomic.Uint64
}

// NewAdmissionWrapper wraps inner with a priority admission queue using cfg.
func NewAdmissionWrapper(inner domain.EndpointSelector, stats ports.StatsCollector, cfg config.AdmissionConfig) *AdmissionWrapper {
	if strings.TrimSpace(cfg.Header) == "" {
		cfg.Header = constants.HeaderXOllaClass
	}
	cfg.DefaultClass = strings.ToLower(strings.TrimSpace(cfg.DefaultClass))
	return &AdmissionWrapper{
		inner: inner,
		stats: stats,
		cfg:   cfg,
	}
}

// Name returns a descriptive name that composes the inner balancer name.
func (a *AdmissionWrapper) Name() string {
	return "admission(" + a.inner.Name() + ")"
}

// QueueSnapshot returns a consistent point-in-time view for operator tooling.
func (a *AdmissionWrapper) QueueSnapshot() QueueSnapshot {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot := QueueSnapshot{
		ByClass: make(map[string]int),
		ByModel: make(map[string]int),
		Items:   make([]QueueItem, 0, len(a.waiters)),
	}
	waiters := make([]*admissionWaiter, 0, len(a.waiters))
	for _, w := range a.waiters {
		if w == nil || w.ctx.Err() != nil {
			continue
		}
		waiters = append(waiters, w)
	}
	sort.SliceStable(waiters, func(i, j int) bool {
		return waiters[i].weight > waiters[j].weight ||
			(waiters[i].weight == waiters[j].weight && waiters[i].seq < waiters[j].seq)
	})
	for _, w := range waiters {
		waited := now.Sub(w.queuedAt)
		if waited < 0 {
			waited = 0
		}
		snapshot.Waiting++
		snapshot.ByClass[w.class]++
		model := w.model
		if model == "" {
			model = "unknown"
		}
		snapshot.ByModel[model]++
		if waited.Milliseconds() > snapshot.OldestWaitMs {
			snapshot.OldestWaitMs = waited.Milliseconds()
		}
		item := QueueItem{
			Position:         snapshot.Waiting,
			RequestID:        w.requestID,
			Model:            w.model,
			Class:            w.class,
			QueuedAt:         w.queuedAt,
			WaitedMs:         waited.Milliseconds(),
			RequestedContext: w.requestedContext,
		}
		if w.hasDeadline {
			deadline := w.deadline
			item.Deadline = &deadline
		}
		if w.waitTimeout > 0 {
			item.TimeoutMs = w.waitTimeout.Milliseconds()
		}
		snapshot.Items = append(snapshot.Items, item)
	}
	return snapshot
}

// IncrementConnections delegates to the inner selector.
func (a *AdmissionWrapper) IncrementConnections(endpoint *domain.Endpoint) {
	a.inner.IncrementConnections(endpoint)
}

// DecrementConnections delegates, then wakes the highest-weight waiter so the
// freed slot is offered to the queue rather than the next arrival.
func (a *AdmissionWrapper) DecrementConnections(endpoint *domain.Endpoint) {
	a.inner.DecrementConnections(endpoint)
	a.wakeNext()
}

// Select waits for an idle slot when every candidate is at num_parallel,
// preferring higher-weight waiters, then delegates to the inner selector.
func (a *AdmissionWrapper) Select(ctx context.Context, endpoints []*domain.Endpoint) (*domain.Endpoint, error) {
	header, _ := ctx.Value(constants.ContextAdmissionHeaderKey).(string)
	ip, _ := ctx.Value(constants.ContextClientIPKey).(string)
	class, source, weight := ResolveAdmissionClass(ip, header, a.cfg)

	outcome, _ := ctx.Value(constants.ContextAdmissionOutcomeKey).(*domain.AdmissionOutcome)
	if outcome != nil {
		outcome.Class = class
		outcome.Source = source
	}

	started := time.Now()
	if a.tryAdmit(endpoints, weight) {
		return a.inner.Select(ctx, endpoints)
	}

	w := a.enqueue(ctx, class, weight)
	defer a.removeWaiter(w)

	slog.Info("admission waiting for idle slot",
		"class", class,
		"source", source,
		"weight", weight,
		"waiters", a.waiterCount())

	var timer <-chan time.Time
	if a.cfg.WaitTimeout > 0 {
		t := time.NewTimer(a.cfg.WaitTimeout)
		defer t.Stop()
		timer = t.C
	}

	select {
	case <-w.ready:
		if outcome != nil {
			outcome.Waited = time.Since(started)
		}
		slog.Info("admission granted after wait",
			"class", class,
			"waited_ms", time.Since(started).Milliseconds())
		return a.inner.Select(ctx, endpoints)
	case <-timer:
		if outcome != nil {
			outcome.Waited = time.Since(started)
		}
		slog.Info("admission wait timeout, dispatching busy-warm",
			"class", class,
			"waited_ms", time.Since(started).Milliseconds())
		return a.inner.Select(ctx, endpoints)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ResolveAdmissionClass maps a client to a class. Header wins when it names a
// known class; otherwise the first matching CIDR; otherwise DefaultClass.
func ResolveAdmissionClass(ip, header string, cfg config.AdmissionConfig) (class, source string, weight int) {
	header = strings.ToLower(strings.TrimSpace(header))
	if header != "" {
		if w, ok := classWeight(cfg, header); ok {
			return header, admissionSourceHeader, w
		}
	}
	if parsed := net.ParseIP(strings.TrimSpace(ip)); parsed != nil {
		for _, entry := range cfg.CIDRs {
			_, network, err := net.ParseCIDR(entry.CIDR)
			if err != nil {
				continue
			}
			if network.Contains(parsed) {
				c := strings.ToLower(strings.TrimSpace(entry.Class))
				if w, ok := classWeight(cfg, c); ok {
					return c, admissionSourceCIDR, w
				}
			}
		}
	}
	def := strings.ToLower(strings.TrimSpace(cfg.DefaultClass))
	w, _ := classWeight(cfg, def)
	return def, admissionSourceDefault, w
}

func classWeight(cfg config.AdmissionConfig, name string) (int, bool) {
	if name == "" || cfg.Classes == nil {
		return 0, false
	}
	if class, ok := cfg.Classes[name]; ok {
		return class.Weight, true
	}
	for k, class := range cfg.Classes {
		if strings.EqualFold(k, name) {
			return class.Weight, true
		}
	}
	return 0, false
}

func (a *AdmissionWrapper) tryAdmit(endpoints []*domain.Endpoint, weight int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasIdleSlotLocked(endpoints) {
		return false
	}
	if maxW, ok := a.maxWaiterWeightLocked(); ok && weight <= maxW {
		return false
	}
	return true
}

func (a *AdmissionWrapper) hasIdleSlotLocked(endpoints []*domain.Endpoint) bool {
	if a.stats == nil {
		return true
	}
	for _, endpoint := range endpoints {
		if endpoint == nil || !endpoint.Status.IsRoutable() {
			continue
		}
		if a.stats.GetConnectionCount(endpoint.URLString) < int64(numParallelSlots(endpoint)) {
			return true
		}
	}
	return false
}

func (a *AdmissionWrapper) maxWaiterWeightLocked() (int, bool) {
	maxW := 0
	found := false
	for _, w := range a.waiters {
		if w == nil || w.ctx.Err() != nil {
			continue
		}
		if !found || w.weight > maxW {
			maxW = w.weight
			found = true
		}
	}
	return maxW, found
}

func (a *AdmissionWrapper) enqueue(ctx context.Context, class string, weight int) *admissionWaiter {
	n := a.seq.Add(1)
	w := &admissionWaiter{
		id:          n,
		seq:         n,
		class:       class,
		weight:      weight,
		ready:       make(chan struct{}),
		ctx:         ctx,
		queuedAt:    time.Now(),
		waitTimeout: a.cfg.WaitTimeout,
	}
	if model, ok := ctx.Value(constants.ContextModelKey).(string); ok {
		w.model = model
	}
	if requestID, ok := ctx.Value(constants.ContextRequestIdKey).(string); ok {
		w.requestID = requestID
	}
	if requestedContext, ok := ctx.Value(constants.ContextNumCtxKey).(int); ok {
		w.requestedContext = requestedContext
	}
	if deadline, ok := ctx.Deadline(); ok {
		w.deadline = deadline
		w.hasDeadline = true
	}
	a.mu.Lock()
	a.waiters = append(a.waiters, w)
	a.mu.Unlock()
	return w
}

func (a *AdmissionWrapper) removeWaiter(target *admissionWaiter) {
	if target == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, w := range a.waiters {
		if w != nil && w.id == target.id {
			a.waiters = append(a.waiters[:i], a.waiters[i+1:]...)
			return
		}
	}
}

func (a *AdmissionWrapper) waiterCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.waiters)
}

func (a *AdmissionWrapper) wakeNext() {
	a.mu.Lock()
	defer a.mu.Unlock()
	idx := a.highestWaiterIndexLocked()
	if idx < 0 {
		return
	}
	w := a.waiters[idx]
	a.waiters = append(a.waiters[:idx], a.waiters[idx+1:]...)
	close(w.ready)
}

func (a *AdmissionWrapper) highestWaiterIndexLocked() int {
	best := -1
	for i, w := range a.waiters {
		if w == nil || w.ctx.Err() != nil {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		cur := a.waiters[best]
		if w.weight > cur.weight || (w.weight == cur.weight && w.seq < cur.seq) {
			best = i
		}
	}
	return best
}
