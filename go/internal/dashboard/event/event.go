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
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
)

// EventAgent is the dashboard event module. It implements the agent.Module
// contract and owns the monitor and report loops (monitor.go/report.go) and the
// internal states endpoint.
type EventAgent struct {
	// eventDir is <log_dir>/events, the directory monitored for event logs.
	eventDir string
	// gcs is used to lazily resolve the head dashboard HTTP address.
	gcs gcs.Client

	// queue holds batches of event lines read by the monitor.
	queue chan []string

	// totalEventReported is the cumulative number of events reported.
	totalEventReported atomic.Int64
	// totalRequestSent is the cumulative number of report requests sent.
	totalRequestSent atomic.Int64
	// droppedBatches is the cumulative number of event batches dropped because
	// the report queue was full (see monitorEvents). It keeps monitoring
	// decoupled from a slow or hung report loop.
	droppedBatches atomic.Int64
	// moduleStarted records when the module was created.
	moduleStarted time.Time

	// dashboardHTTPAddress is lazily resolved on first use and never changed.
	dashboardHTTPAddress atomic.Value
	// addressResolved reports whether dashboardHTTPAddress has been set.
	addressResolved atomic.Bool
}

// Compile-time assertion that EventAgent satisfies the module contract.
var _ agent.Module = (*EventAgent)(nil)

// New builds an EventAgent from the agent config: the events directory is
// derived from the log directory, the queue is created with
// EVENT_AGENT_CACHE_SIZE capacity, and counters/start time are initialized. A
// missing log directory or GCS client is a configuration error.
func New(cfg agent.Config) (*EventAgent, error) {
	if cfg.LogDir == "" {
		return nil, errors.New("log dir is empty")
	}
	if cfg.GCS == nil {
		return nil, errors.New("gcs client is nil")
	}
	return &EventAgent{
		eventDir:      filepath.Join(cfg.LogDir, "events"),
		gcs:           cfg.GCS,
		queue:         make(chan []string, eventAgentCacheSize),
		moduleStarted: time.Now(),
	}, nil
}

// Start creates the events directory and starts the monitor and report loops,
// which run until ctx is cancelled. Loop errors are logged by a background
// goroutine, so a failed scan or report never crashes the agent.
func (e *EventAgent) Start(ctx context.Context) error {
	if err := os.MkdirAll(e.eventDir, 0o755); err != nil {
		return err
	}
	log.Log.Info("event agent started",
		"event_dir", e.eventDir,
		"cache_size", eventAgentCacheSize,
		"report_interval_seconds", eventAgentReportIntervalSeconds())

	go func() {
		if err := monitorEvents(ctx, e.eventDir, e.queue, &e.droppedBatches); err != nil && !errors.Is(err, context.Canceled) {
			log.Log.Error(err, "event monitor loop stopped", "event_dir", e.eventDir)
		}
	}()
	go func() {
		if err := e.reportEvents(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Log.Error(err, "event report loop stopped")
		}
	}()
	return nil
}

// RegisterGRPC registers no services; the Python EventAgent exposes no gRPC API
// either.
func (e *EventAgent) RegisterGRPC(*grpc.Server) error { return nil }

// RegisterHTTP registers the internal states endpoint.
func (e *EventAgent) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("/api/event_internal_states", e.handleInternalStates)
	return nil
}
