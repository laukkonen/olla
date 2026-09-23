package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/thushan/olla/internal/adapter/proxy/core"
	"github.com/thushan/olla/internal/config"
)

const (
	defaultModelGroupTextBytes    = 8192
	maxModelGroupRequestBodyBytes = 1 << 20
)

type modelGroupClassifierRequest struct {
	Group string `json:"group"`
	Text  string `json:"text"`
}

type modelGroupClassifierResponse struct {
	Member     string  `json:"member"`
	Confidence float64 `json:"confidence"`
}

type modelGroupClassifierResult struct {
	member     string
	confidence float64
	latency    time.Duration
	reason     string
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

	member, source, reason := group.CapableModel, "fallback", "classifier_not_configured"
	if capableReason := modelGroupCapableReason(pr); capableReason != "" {
		source, reason = "rule", capableReason
	} else if group.Classifier.URL != "" {
		if text, inputReason := modelGroupText(r, group.Classifier.MaxTextBytes); inputReason == "" {
			result := classifyModelGroup(ctx, group, pr.model, text)
			pr.modelGroupClassifierAttempted = true
			pr.modelGroupClassifierConfidence = result.confidence
			pr.modelGroupClassifierLatency = result.latency
			if result.member != "" {
				member, source, reason = result.member, "classifier", result.reason
			} else {
				reason = result.reason
			}
		} else {
			source, reason = "rule", inputReason
		}
	}

	pr.modelGroup = pr.model
	pr.modelGroupMember = member
	pr.modelGroupSource = source
	pr.modelGroupReason = reason
	pr.model = member
	pr.stats.Model = member
	if pr.profile != nil {
		pr.profile.ModelName = member
	}
	core.RewriteRequestModel(r, member)
}

func modelGroupCapableReason(pr *proxyRequest) string {
	if pr.profile == nil {
		return ""
	}
	if pr.profile.RequiresVision {
		return "vision"
	}
	if pr.profile.RequestedContext > 32768 {
		return "long_context"
	}
	return ""
}

func modelGroupText(r *http.Request, maxBytes int) (string, string) {
	if r.Body == nil || r.ContentLength == 0 {
		return "", "empty_body"
	}
	if maxBytes <= 0 {
		maxBytes = defaultModelGroupTextBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxModelGroupRequestBodyBytes+1))
	if len(body) > 0 {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
	}
	if err != nil || len(body) == 0 {
		return "", "read_error"
	}
	if len(body) > maxModelGroupRequestBodyBytes {
		return "", "request_too_large"
	}

	var request struct {
		Prompt    string            `json:"prompt"`
		Messages  []json.RawMessage `json:"messages"`
		Tools     json.RawMessage   `json:"tools"`
		Functions json.RawMessage   `json:"functions"`
	}
	if json.Unmarshal(body, &request) != nil {
		return "", "invalid_request"
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
			return "", "unsupported_message"
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
			return "", "unsupported_content"
		}
		for _, part := range contentParts {
			if part.Type != "text" || part.Text == "" {
				return "", "unsupported_content"
			}
			parts = append(parts, part.Text)
		}
	}
	text := strings.TrimSpace(strings.Join(parts, "\n"))
	if text == "" {
		return "", "empty_text"
	}
	if len(text) > maxBytes {
		text = text[len(text)-maxBytes:]
		for len(text) > 0 && !utf8.RuneStart(text[0]) {
			text = text[1:]
		}
	}
	return text, ""
}

func classifyModelGroup(ctx context.Context, group config.ModelGroupConfig, groupName, text string) (result modelGroupClassifierResult) {
	started := time.Now()
	defer func() { result.latency = time.Since(started) }()
	timeout := group.Classifier.Timeout
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	requestBody, err := json.Marshal(modelGroupClassifierRequest{Group: groupName, Text: text})
	if err != nil {
		result.reason = "classifier_encode_error"
		return result
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, group.Classifier.URL, bytes.NewReader(requestBody))
	if err != nil {
		result.reason = "classifier_request_error"
		return result
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{}).Do(req)
	if err != nil {
		result.reason = "classifier_unavailable"
		return result
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		result.reason = "classifier_http_error"
		return result
	}
	var responseResult modelGroupClassifierResponse
	if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&responseResult) != nil {
		result.reason = "classifier_invalid_response"
		return result
	}
	result.confidence = responseResult.Confidence
	if responseResult.Confidence < group.Classifier.MinConfidence {
		result.reason = "classifier_low_confidence"
		return result
	}
	switch responseResult.Member {
	case "fast":
		result.member, result.reason = group.FastModel, "classifier_selected"
	case "capable":
		result.member, result.reason = group.CapableModel, "classifier_selected"
	default:
		result.reason = "classifier_invalid_member"
	}
	return result
}
