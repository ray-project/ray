// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package event

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/log"
)

// resolveDashboardAddress lazily gets the head dashboard HTTP address from GCS
// InternalKV, retrying until found; once resolved the address is cached.
func (e *EventAgent) resolveDashboardAddress(ctx context.Context) (string, error) {
	for {
		if e.addressResolved.Load() {
			addr, _ := e.dashboardHTTPAddress.Load().(string)
			return addr, nil
		}
		rpcCtx, rpcCancel := context.WithTimeout(ctx, gcsRpcTimeoutSeconds())
		data, err := e.gcs.Get(rpcCtx, agent.KVNamespaceDashboard, common.DASHBOARD_ADDRESS)
		rpcCancel()
		if err == nil && len(data) > 0 {
			addr := string(data)
			// The Python head writes the KV value as a bare host:port
			// (build_address), while the Go head writes a full URL. Normalize a
			// scheme-less address so the report request has a parseable URL
			// (python event_agent normalizes the same way after reading the KV).
			if !strings.Contains(addr, "://") {
				addr = "http://" + addr
			}
			e.dashboardHTTPAddress.Store(addr)
			e.addressResolved.Store(true)
			log.Log.Info("Got dashboard http address from GCS", "address", addr)
			return addr, nil
		}
		if err != nil {
			log.Log.Error(err, "Get dashboard http address failed.")
		} else {
			log.Log.Error(errors.New("Dashboard http address not found in InternalKV."),
				"Get dashboard http address failed.")
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// reportEvents drains batches from the queue and reports them to the head
// dashboard, retrying EVENT_AGENT_RETRY_TIMES times before dropping a batch.
// It runs until ctx is cancelled or the queue is closed.
func (e *EventAgent) reportEvents(ctx context.Context) error {
	for {
		if err := e.reportBatch(ctx); err != nil {
			if errors.Is(err, errQueueClosed) {
				return err
			}
		}
		if !e.waitReportInterval(ctx) {
			return ctx.Err()
		}
	}
}

// reportBatch picks one batch from the queue and reports it. It resolves the
// head address, gets a batch, POSTs it with the retry loop, and on retry
// exhaustion drops the batch with a truncated error log. It reports
// errQueueClosed when the queue is closed and nil otherwise.
func (e *EventAgent) reportBatch(ctx context.Context) error {
	addr, err := e.resolveDashboardAddress(ctx)
	if err != nil {
		return nil
	}

	data, err := e.pickBatch(ctx)
	if err != nil {
		return err
	}
	e.totalEventReported.Add(int64(len(data)))

	body, err := json.Marshal(data)
	if err != nil {
		log.Log.Error(err, "failed to marshal events for reporting")
		return nil
	}

	if err := e.postBatch(ctx, addr, body); err != nil {
		dataStr := string(body)
		msg := dataStr
		if len(dataStr) > logErrorEventStringLengthLimit {
			msg = dataStr[:logErrorEventStringLengthLimit] + "..."
		}
		log.Log.Error(err, "Report event failed", "data", msg)
		return nil
	}
	e.totalRequestSent.Add(1)
	return nil
}

// errQueueClosed is returned by pickBatch when the queue has been closed.
var errQueueClosed = errors.New("event queue closed")

// waitReportInterval waits one report interval, returning false when ctx is
// cancelled during the wait.
func (e *EventAgent) waitReportInterval(ctx context.Context) bool {
	timer := time.NewTimer(time.Duration(eventAgentReportIntervalSeconds() * float64(time.Second)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// pickBatch waits for the next batch of event lines from the queue.
func (e *EventAgent) pickBatch(ctx context.Context) ([]string, error) {
	select {
	case data, ok := <-e.queue:
		if !ok {
			return nil, errQueueClosed
		}
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// postBatch POSTs body to {addr}/report_events, retrying up to
// EVENT_AGENT_RETRY_TIMES times with exponential backoff between attempts,
// returning the last error when every attempt failed.
func (e *EventAgent) postBatch(ctx context.Context, addr string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < eventAgentRetryTimes; attempt++ {
		lastErr = e.postOnce(ctx, addr, body)
		if lastErr == nil {
			return nil
		}
		log.Log.Error(nil, "Report event failed, retrying...",
			"error", lastErr.Error(), "attempt", attempt+1, "address", addr)
		if !e.waitRetryBackoff(ctx, attempt) {
			return ctx.Err()
		}
	}
	return lastErr
}

// waitRetryBackoff sleeps the backoff delay for attempt, returning false when
// ctx is cancelled during the wait.
func (e *EventAgent) waitRetryBackoff(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(retryBackoffDelay(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// retryBackoffDelay returns the jittered exponential backoff for the given
// attempt: base * 2^attempt capped at reportRetryMaxDelay, with full jitter in
// [0, cap] to avoid a synchronized thundering herd across agents/attempts.
func retryBackoffDelay(attempt int) time.Duration {
	exp := reportRetryBaseDelay << uint(min(attempt, 30))
	if exp > reportRetryMaxDelay || exp < reportRetryBaseDelay {
		exp = reportRetryMaxDelay
	}
	return time.Duration(rand.Int63n(int64(exp)))
}

// postOnce sends one report request, returning an error on transport failure
// or a non-2xx response.
func (e *EventAgent) postOnce(ctx context.Context, addr string, body []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, reportRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, addr+"/report_events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// /report_events is not a public head path, so on RAY_AUTH_MODE=token
	// clusters every report needs the bearer token (python event_agent sends
	// get_auth_headers_if_auth_enabled({}) on the report request).
	if os.Getenv("RAY_AUTH_MODE") == "token" {
		if token := reportAuthToken(); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("report events request failed: " + resp.Status)
	}
	return nil
}

// reportAuthToken returns the bearer token for report requests, mirroring the
// head's token precedence (RAY_AUTH_TOKEN env, then the first line of
// RAY_AUTH_TOKEN_PATH). Empty when neither is set.
func reportAuthToken() string {
	if env := os.Getenv("RAY_AUTH_TOKEN"); env != "" {
		return env
	}
	path := os.Getenv("RAY_AUTH_TOKEN_PATH")
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// getInternalStates returns the module internal states: 204 No Content until
// both a report and a request have been counted, otherwise 200 JSON with the
// cumulative counters, queue length and uptime.
func (e *EventAgent) getInternalStates() ([]byte, bool) {
	total := e.totalEventReported.Load()
	requests := e.totalRequestSent.Load()
	if total <= 0 || requests <= 0 {
		return nil, false
	}
	states := map[string]interface{}{
		"total_events_reported": total,
		"Total_report_request":  requests,
		"queue_size":            len(e.queue),
		"queue_dropped_batches": e.droppedBatches.Load(),
		"total_uptime":          time.Since(e.moduleStarted).Seconds(),
	}
	body, err := json.Marshal(states)
	if err != nil {
		return nil, false
	}
	return body, true
}

// handleInternalStates serves GET /api/event_internal_states: 204 No Content
// until the first event/request has been reported, then 200 with a JSON body.
func (e *EventAgent) handleInternalStates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "405: Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	body, ok := e.getInternalStates()
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
