package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/adapter/balancer"
)

func TestQueueStatusHandlerExposesSnapshotWithoutClientIP(t *testing.T) {
	app := &Application{
		queueSnapshotFn: func() balancer.QueueSnapshot {
			return balancer.QueueSnapshot{
				Waiting:      1,
				OldestWaitMs: 42,
				ByClass:      map[string]int{"interactive": 1},
				ByModel:      map[string]int{"llama3:8b": 1},
				Items: []balancer.QueueItem{{
					Position: 1, Model: "llama3:8b", Class: "interactive",
					QueuedAt: time.Now(), WaitedMs: 42,
				}},
			}
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/status/queue", nil)
	res := httptest.NewRecorder()
	app.queueStatusHandler(res, req)
	require.Equal(t, http.StatusOK, res.Code)
	require.Equal(t, "application/json", res.Header().Get("Content-Type"))
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &body))
	require.Equal(t, float64(1), body["waiting"])
	require.NotContains(t, res.Body.String(), "client_ip")
}
