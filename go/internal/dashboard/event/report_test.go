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
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
)

// fakeGCS is a gcs.Client stub: it only overrides Get and satisfies the rest
// of the interface through the embedded nil gcs.Client, which the event module
// never invokes.
type fakeGCS struct {
	gcs.Client
	// dashboardAddr is the value Get returns for the dashboard address key
	// under the dashboard namespace.
	dashboardAddr string
	// err is returned by Get when non-nil.
	err error
	// calls counts the Get invocations and is used to prove the cached
	// address is not re-resolved.
	calls int
	mu    sync.Mutex
}

func (f *fakeGCS) Get(_ context.Context, ns, key string) ([]byte, error) {
	// Read both mutable fields under the lock: dashboardAddr is written by
	// TestResolveDashboardAddressNotSetThenAvailable while a resolver goroutine
	// is concurrently calling Get.
	f.mu.Lock()
	f.calls++
	addr := f.dashboardAddr
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if ns == agent.KVNamespaceDashboard && key == common.DASHBOARD_ADDRESS {
		return []byte(addr), nil
	}
	return nil, nil
}

// newEventAgent builds an EventAgent over the given fake GCS.
func newEventAgent(t *testing.T, f *fakeGCS) *EventAgent {
	t.Helper()
	e, err := New(agent.Config{LogDir: t.TempDir(), GCS: f})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

// startReportLoop runs reportEvents until cancel is called; it returns the
// error reportEvents returned through done.
func startReportLoop(t *testing.T, e *EventAgent) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.reportEvents(ctx) }()
	return cancel, done
}

func mustCancelReportLoop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("reportEvents: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reportEvents did not return after cancellation")
	}
}

func TestResolveDashboardAddressCached(t *testing.T) {
	f := &fakeGCS{dashboardAddr: "http://127.0.0.1:8265"}
	e := newEventAgent(t, f)

	addr, err := e.resolveDashboardAddress(context.Background())
	if err != nil {
		t.Fatalf("resolveDashboardAddress: %v", err)
	}
	callsAfterFirst := f.calls
	addr2, err := e.resolveDashboardAddress(context.Background())
	if err != nil {
		t.Fatalf("second resolveDashboardAddress: %v", err)
	}
	if addr2 != addr {
		t.Errorf("second resolve = %q, want cached %q", addr2, addr)
	}
	if f.calls != callsAfterFirst {
		t.Errorf("GCS Get called %d times after cache, want 0 (address must be cached)", f.calls-callsAfterFirst)
	}
}

// TestResolveDashboardAddressErrorRetries checks that a GCS Get error keeps
// the resolver retrying (with a 1s sleep, mirroring _get_dashboard_http_address)
// until the context is cancelled.
func TestResolveDashboardAddressErrorRetries(t *testing.T) {
	f := &fakeGCS{err: errors.New("gcs unreachable")}
	e := newEventAgent(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := e.resolveDashboardAddress(ctx); result <- err }()

	select {
	case <-result:
		t.Fatal("resolveDashboardAddress returned before context cancellation")
	case <-time.After(1500 * time.Millisecond):
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Errorf("resolveDashboardAddress err = %v, want context.Canceled", err)
	}
	if f.calls < 1 {
		t.Error("GCS Get was never called")
	}
}

// TestResolveDashboardAddressNotSetThenAvailable checks the "address not in
// InternalKV yet" path: an empty Get result is retried every 1s until the
// address appears in GCS, then cached.
func TestResolveDashboardAddressNotSetThenAvailable(t *testing.T) {
	f := &fakeGCS{}
	e := newEventAgent(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		addr, err := e.resolveDashboardAddress(ctx)
		if err != nil {
			result <- err
			return
		}
		f.mu.Lock()
		f.dashboardAddr = addr
		f.mu.Unlock()
		result <- nil
	}()

	// Wait for at least one empty result before making the address available.
	time.Sleep(1500 * time.Millisecond)
	f.mu.Lock()
	f.dashboardAddr = "http://127.0.0.1:8265"
	f.mu.Unlock()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("resolveDashboardAddress: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolveDashboardAddress did not return after the address became available")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dashboardAddr != "http://127.0.0.1:8265" {
		t.Errorf("resolved address = %q, want the GCS value", f.dashboardAddr)
	}
}

// headRecorder collects the batches posted to a fake head /report_events
// endpoint.
type headRecorder struct {
	mu        sync.Mutex
	batches   [][]string
	rawBodies [][]byte
	status    int
	failures  int
}

// handler returns an http.HandlerFunc that rejects the first failures requests
// with status and records the rest.
func (h *headRecorder) handler(failures int, status int) http.HandlerFunc {
	h.mu.Lock()
	h.failures = failures
	h.status = status
	h.mu.Unlock()
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		if h.failures > 0 {
			h.failures--
			s := h.status
			h.mu.Unlock()
			w.WriteHeader(s)
			return
		}
		h.mu.Unlock()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var batch []string
		if err := json.Unmarshal(body, &batch); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		h.batches = append(h.batches, batch)
		h.rawBodies = append(h.rawBodies, body)
		h.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (h *headRecorder) recorded() (int, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var lines []string
	for _, b := range h.batches {
		lines = append(lines, b...)
	}
	return len(h.batches), lines
}

// waitReported blocks until at least want lines were received by the head.
func waitReported(t *testing.T, rec *headRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, lines := rec.recorded()
		if len(lines) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, lines := rec.recorded()
	t.Fatalf("head received %d lines (%v), want %d", len(lines), lines, want)
}

// TestReportEventsPostsBatches mirrors the happy path of report_events
// (event_agent.py): batches drained from the queue are POSTed as a JSON string
// array, the counters are updated and the address is resolved from GCS.
func TestReportEventsPostsBatches(t *testing.T) {
	rec := &headRecorder{}
	srv := httptest.NewServer(rec.handler(0, 0))
	defer srv.Close()
	f := &fakeGCS{dashboardAddr: srv.URL}
	e := newEventAgent(t, f)

	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`, `{"a":2}`}
	e.queue <- []string{`{"a":3}`}
	waitReported(t, rec, 3)
	mustCancelReportLoop(t, cancel, done)

	if e.totalEventReported.Load() != 3 {
		t.Errorf("totalEventReported = %d, want 3", e.totalEventReported.Load())
	}
	if e.totalRequestSent.Load() != 2 {
		t.Errorf("totalRequestSent = %d, want 2", e.totalRequestSent.Load())
	}

	// The request body must be a JSON array of the raw event lines.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i, raw := range rec.rawBodies {
		if len(raw) == 0 || raw[0] != '[' {
			t.Errorf("batch %d body starts with %q, want '['", i, string(raw[:min(len(raw), 8)]))
		}
	}
	if len(rec.batches) != 2 {
		t.Errorf("batches received = %d, want 2", len(rec.batches))
	}
}

// TestResolveDashboardAddressNormalizesBareHostPort checks that a scheme-less
// KV value (what the Python head writes via build_address) is normalized to a
// full URL before it is cached, so the report request has a parseable URL.
func TestResolveDashboardAddressNormalizesBareHostPort(t *testing.T) {
	f := &fakeGCS{dashboardAddr: "127.0.0.1:8265"}
	e := newEventAgent(t, f)

	addr, err := e.resolveDashboardAddress(context.Background())
	if err != nil {
		t.Fatalf("resolveDashboardAddress: %v", err)
	}
	if addr != "http://127.0.0.1:8265" {
		t.Errorf("resolved address = %q, want %q", addr, "http://127.0.0.1:8265")
	}
	cached, _ := e.dashboardHTTPAddress.Load().(string)
	if cached != "http://127.0.0.1:8265" {
		t.Errorf("cached address = %q, want %q", cached, "http://127.0.0.1:8265")
	}
}

// TestResolveDashboardAddressKeepsScheme checks a KV value that already carries
// a scheme is cached unchanged.
func TestResolveDashboardAddressKeepsScheme(t *testing.T) {
	for _, want := range []string{"http://127.0.0.1:8265", "https://head.example:443"} {
		f := &fakeGCS{dashboardAddr: want}
		e := newEventAgent(t, f)
		addr, err := e.resolveDashboardAddress(context.Background())
		if err != nil {
			t.Fatalf("resolveDashboardAddress(%q): %v", want, err)
		}
		if addr != want {
			t.Errorf("resolved address = %q, want unchanged %q", addr, want)
		}
	}
}

// TestReportEventsPostsToSchemeLessAddress checks the end-to-end effect of the
// normalization: the KV carries a bare host:port (the Python head form) and the
// report POST must still reach the httptest server.
func TestReportEventsPostsToSchemeLessAddress(t *testing.T) {
	rec := &headRecorder{}
	srv := httptest.NewServer(rec.handler(0, 0))
	defer srv.Close()
	// Strip the "http://" scheme so the KV value is what the Python head writes.
	hostPort := strings.TrimPrefix(srv.URL, "http://")
	f := &fakeGCS{dashboardAddr: hostPort}
	e := newEventAgent(t, f)

	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`}
	waitReported(t, rec, 1)
	mustCancelReportLoop(t, cancel, done)

	if e.totalRequestSent.Load() != 1 {
		t.Errorf("totalRequestSent = %d, want 1", e.totalRequestSent.Load())
	}
}

// TestPostOnceSendsAuthHeader checks that when RAY_AUTH_MODE=token the report
// POST carries the bearer token (Python get_auth_headers_if_auth_enabled).
func TestPostOnceSendsAuthHeader(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret-token")

	var (
		mu     sync.Mutex
		header string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		header = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := newEventAgent(t, &fakeGCS{dashboardAddr: srv.URL})
	if err := e.postOnce(context.Background(), srv.URL, []byte(`["x"]`)); err != nil {
		t.Fatalf("postOnce: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if header != "Bearer secret-token" {
		t.Errorf("Authorization header = %q, want %q", header, "Bearer secret-token")
	}
}

// TestPostOnceNoAuthHeaderWhenDisabled checks that without RAY_AUTH_MODE=token
// no Authorization header is attached, even when a token is present.
func TestPostOnceNoAuthHeaderWhenDisabled(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "")
	t.Setenv("RAY_AUTH_TOKEN", "secret-token")

	var (
		mu     sync.Mutex
		header string
		sent   bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		header = r.Header.Get("Authorization")
		sent = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := newEventAgent(t, &fakeGCS{dashboardAddr: srv.URL})
	if err := e.postOnce(context.Background(), srv.URL, []byte(`["x"]`)); err != nil {
		t.Fatalf("postOnce: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !sent {
		t.Fatal("report request was not received")
	}
	if header != "" {
		t.Errorf("Authorization header = %q, want empty when auth is disabled", header)
	}
}

// TestPostOnceAuthTokenFromFile checks the token precedence fallback: with no
// RAY_AUTH_TOKEN the first non-empty trimmed line of RAY_AUTH_TOKEN_PATH is used.
func TestPostOnceAuthTokenFromFile(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "")
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("\n  file-token  \nsecond\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RAY_AUTH_TOKEN_PATH", path)

	var (
		mu     sync.Mutex
		header string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		header = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	e := newEventAgent(t, &fakeGCS{dashboardAddr: srv.URL})
	if err := e.postOnce(context.Background(), srv.URL, []byte(`["x"]`)); err != nil {
		t.Fatalf("postOnce: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if header != "Bearer file-token" {
		t.Errorf("Authorization header = %q, want %q", header, "Bearer file-token")
	}
}

// TestReportEventsFailsThenRecovers checks the retry path (Python "Report
// event failed, retrying..."): a batch whose first requests are rejected is
// retried and eventually accepted, so total_request_sent is incremented once.
func TestReportEventsFailsThenRecovers(t *testing.T) {
	rec := &headRecorder{}
	srv := httptest.NewServer(rec.handler(2, http.StatusInternalServerError))
	defer srv.Close()
	f := &fakeGCS{dashboardAddr: srv.URL}
	e := newEventAgent(t, f)

	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`, `{"a":2}`}
	waitReported(t, rec, 2)
	mustCancelReportLoop(t, cancel, done)

	n, _ := rec.recorded()
	if n != 1 {
		t.Errorf("batches received = %d, want 1 (the retried batch)", n)
	}
	if e.totalEventReported.Load() != 2 {
		t.Errorf("totalEventReported = %d, want 2", e.totalEventReported.Load())
	}
	if e.totalRequestSent.Load() != 1 {
		t.Errorf("totalRequestSent = %d, want 1", e.totalRequestSent.Load())
	}
}

// TestReportEventsDropsBatchAfterRetries checks that a batch is dropped with
// an error log once the EVENT_AGENT_RETRY_TIMES attempts fail, mirroring the
// else branch of report_events.
func TestReportEventsDropsBatchAfterRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	f := &fakeGCS{dashboardAddr: srv.URL}
	e := newEventAgent(t, f)

	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`}

	// Give the retry loop enough time to exhaust EVENT_AGENT_RETRY_TIMES
	// attempts, then verify the batch was dropped.
	time.Sleep(2 * time.Second)
	if e.totalRequestSent.Load() != 0 {
		t.Errorf("totalRequestSent = %d, want 0 (batch must be dropped)", e.totalRequestSent.Load())
	}
	if e.totalEventReported.Load() != 1 {
		t.Errorf("totalEventReported = %d, want 1 (counted once on pickup)", e.totalEventReported.Load())
	}
	mustCancelReportLoop(t, cancel, done)
}

// TestRetryBackoffDelayCappedAndJittered checks that the retry backoff stays
// non-negative, is always strictly below the cap (full jitter in [0, exp)), and
// never exceeds reportRetryMaxDelay even for very large attempt counts.
func TestRetryBackoffDelayCappedAndJittered(t *testing.T) {
	for attempt := 0; attempt < 40; attempt++ {
		for i := 0; i < 100; i++ {
			d := retryBackoffDelay(attempt)
			if d < 0 || d >= reportRetryMaxDelay {
				t.Fatalf("attempt %d delay = %v, want in [0, %v)", attempt, d, reportRetryMaxDelay)
			}
		}
	}
}

// TestGetInternalStates204 checks the 204 No Content semantics, matching
// get_internal_states (event_agent.py) returning None (204) when no report and
// no request have been counted.
func TestGetInternalStates204(t *testing.T) {
	e := newEventAgent(t, &fakeGCS{})
	if body, ok := e.getInternalStates(); ok {
		t.Fatalf("getInternalStates returned content %q before any report", body)
	}
}

// TestGetInternalStatesReportedButNoRequest checks the boundary of the Python
// "or" in get_internal_states (event_agent.py): a positive total_event_reported
// with total_request_sent still zero yields 204 No Content, because a request
// has not yet been counted.
func TestGetInternalStatesReportedButNoRequest(t *testing.T) {
	e := newEventAgent(t, &fakeGCS{})
	e.totalEventReported.Add(1)
	if body, ok := e.getInternalStates(); ok {
		t.Fatalf("getInternalStates returned content %q while total_request_sent == 0", body)
	}
}

// TestGetInternalStatesFields checks the reported fields: once a request has
// been counted the counters, queue length and uptime are present with the
// exact Python field names.
func TestGetInternalStatesFields(t *testing.T) {
	rec := &headRecorder{}
	srv := httptest.NewServer(rec.handler(0, 0))
	defer srv.Close()
	f := &fakeGCS{dashboardAddr: srv.URL}
	e := newEventAgent(t, f)

	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`, `{"a":2}`}
	e.queue <- []string{`{"a":3}`}
	waitReported(t, rec, 3)
	// Let the last batch be fully consumed so queue_size is deterministic.
	time.Sleep(300 * time.Millisecond)

	states, ok := e.getInternalStates()
	if !ok {
		t.Fatal("getInternalStates returned no content after a report")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(states, &m); err != nil {
		t.Fatalf("states are not valid JSON: %v", err)
	}
	if total, _ := m["total_events_reported"].(float64); total != 3 {
		t.Errorf("total_events_reported = %v, want 3", m["total_events_reported"])
	}
	if reqs, _ := m["Total_report_request"].(float64); reqs != 2 {
		t.Errorf("Total_report_request = %v, want 2", m["Total_report_request"])
	}
	if qsize, _ := m["queue_size"].(float64); qsize != 0 {
		t.Errorf("queue_size = %v, want 0 after all batches were consumed", m["queue_size"])
	}
	if uptime, _ := m["total_uptime"].(float64); uptime <= 0 {
		t.Errorf("total_uptime = %v, want positive", m["total_uptime"])
	}

	mustCancelReportLoop(t, cancel, done)
}
