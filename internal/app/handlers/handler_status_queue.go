package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/thushan/olla/internal/adapter/balancer"
)

type queueStatusResponse struct {
	Timestamp time.Time `json:"timestamp"`
	balancer.QueueSnapshot
}

func (a *Application) queueStatusHandler(w http.ResponseWriter, r *http.Request) {
	queue := balancer.QueueSnapshot{ByClass: map[string]int{}, ByModel: map[string]int{}, Items: []balancer.QueueItem{}}
	if a.queueSnapshotFn != nil {
		queue = a.queueSnapshotFn()
	}
	body, err := json.Marshal(queueStatusResponse{Timestamp: time.Now(), QueueSnapshot: queue})
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
