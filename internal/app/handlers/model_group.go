package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/thushan/olla/internal/adapter/proxy/core"
	"github.com/thushan/olla/internal/config"
)

const defaultModelGroupTextBytes = 8192

type modelGroupClassifierRequest struct {
	Group string `json:"group"`
	Text  string `json:"text"`
}

type modelGroupClassifierResponse struct {
	Member     string  `json:"member"`
	Confidence float64 `json:"confidence"`
}

// resolveModelGroup only handles an explicitly requested group. It never
// changes a real model or ordinary alias request, and all uncertain cases keep
// the group's capable member.
func (a *Application) resolveModelGroup(ctx context.Context, r *http.Request, pr *proxyRequest) {
	if a.Config == nil || pr.model == "" {
		return
	}
	group, ok := a.Config.ModelGroups[pr.model]
	if !ok {
		return
	}

	member, source := group.CapableModel, "fallback"
	if !modelGroupRequiresCapable(pr) && group.Classifier.URL != "" {
		if text, eligible := modelGroupText(r, group.Classifier.MaxTextBytes); eligible {
			if choice, ok := classifyModelGroup(ctx, group, pr.model, text); ok {
				member, source = choice, "classifier"
			}
		} else {
			source = "rule"
		}
	}

	pr.modelGroup = pr.model
	pr.modelGroupMember = member
	pr.modelGroupSource = source
	pr.model = member
	pr.stats.Model = member
	if pr.profile != nil {
		pr.profile.ModelName = member
	}
	core.RewriteRequestModel(r, member)
}

func modelGroupRequiresCapable(pr *proxyRequest) bool {
	if pr.profile == nil {
		return false
	}
	return pr.profile.RequiresVision || pr.profile.RequiresFunctionCall || pr.profile.RequestedContext > 32768
}

func modelGroupText(r *http.Request, maxBytes int) (string, bool) {
	if r.Body == nil || r.ContentLength == 0 {
		return "", false
	}
	if maxBytes <= 0 {
		maxBytes = defaultModelGroupTextBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(maxBytes)+1))
	if len(body) > 0 {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
	}
	if err != nil || len(body) == 0 || len(body) > maxBytes {
		return "", false
	}

	var request struct {
		Prompt    string            `json:"prompt"`
		Messages  []json.RawMessage `json:"messages"`
		Tools     json.RawMessage   `json:"tools"`
		Functions json.RawMessage   `json:"functions"`
	}
	if json.Unmarshal(body, &request) != nil || len(request.Tools) > 0 || len(request.Functions) > 0 {
		return "", false
	}

	parts := make([]string, 0, len(request.Messages)+1)
	if request.Prompt != "" {
		parts = append(parts, request.Prompt)
	}
	for _, raw := range request.Messages {
		var message struct {
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(raw, &message) != nil || len(message.Content) == 0 {
			return "", false
		}
		var content string
		if json.Unmarshal(message.Content, &content) == nil {
			parts = append(parts, content)
			continue
		}
		var contentParts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(message.Content, &contentParts) != nil {
			return "", false
		}
		for _, part := range contentParts {
			if part.Type != "text" || part.Text == "" {
				return "", false
			}
			parts = append(parts, part.Text)
		}
	}
	text := strings.TrimSpace(strings.Join(parts, "\n"))
	return text, text != ""
}

func classifyModelGroup(ctx context.Context, group config.ModelGroupConfig, groupName, text string) (string, bool) {
	timeout := group.Classifier.Timeout
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	requestBody, err := json.Marshal(modelGroupClassifierRequest{Group: groupName, Text: text})
	if err != nil {
		return "", false
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, group.Classifier.URL, bytes.NewReader(requestBody))
	if err != nil {
		return "", false
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{}).Do(req)
	if err != nil {
		return "", false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", false
	}
	var result modelGroupClassifierResponse
	if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil || result.Confidence < group.Classifier.MinConfidence {
		return "", false
	}
	switch result.Member {
	case "fast":
		return group.FastModel, true
	case "capable":
		return group.CapableModel, true
	default:
		return "", false
	}
}
