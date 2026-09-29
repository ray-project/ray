// Copyright 2025 The Ray Authors.
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

package head

import (
	"context"
	"fmt"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
)

// DashboardHead is the main framework of the head process, mirroring the
// Python DashboardHead in dashboard.py.
type DashboardHead struct {
	cfg     *HeadConfig
	client  *GCSClient
	mods    []HeadModule
	metrics *MetricsRegistry
}

// New creates a DashboardHead backed by the given GCS client and modules.
func New(cfg *HeadConfig, client *GCSClient, mods []HeadModule) *DashboardHead {
	return &DashboardHead{cfg: cfg, client: client, mods: mods, metrics: NewMetricsRegistry(cfg)}
}

// gcsCheckAliveInterval is how often the head probes GCS liveness, aligned
// with the Python dashboard head's 5s check interval.
const gcsCheckAliveInterval = 5 * time.Second

// Run runs the head main flow (aligned with the Python DashboardHead.run()):
// 1. start the metrics exporter (unless minimal), 2. start the HTTP server
// (when serving the frontend), 3. publish the dashboard address to GCS KV,
// 4. run the GCS liveness probe and all modules concurrently.
func (h *DashboardHead) Run(ctx context.Context) error {
	// 0. Log every loaded module, aligned with head.py _load_modules which
	// logs "Loading DashboardHeadModule: {cls}." for each module and the
	// loaded count.
	for _, m := range h.mods {
		log.Log.Info("Loading DashboardHeadModule", "name", m.Name())
	}
	log.Log.Info("Loaded dashboard head modules", "count", len(h.mods))

	// 1. Non-minimal: start the metrics export server and publish its address.
	// A failure to start the metrics server only logs, never terminates the
	// head (aligned with the Python dashboard head, which logs the exception
	// and continues).
	if !h.cfg.Minimal {
		if err := h.metrics.Start(ctx, h.cfg.NodeIPAddress, h.cfg.MetricsExportPort); err != nil {
			log.Log.Error(err, "failed to start metrics server")
		} else {
			// DashboardMetricsAddress in GCS KV with an EMPTY namespace
			// (aligned with dashboard.py, which passes namespace=None, and
			// consumed by the metrics_agent reading from the empty namespace).
			metricsAddr := fmt.Sprintf("%s:%d", h.cfg.NodeIPAddress, h.cfg.MetricsExportPort)
			if _, err := h.client.InternalKVPut(ctx, "", "DashboardMetricsAddress", []byte(metricsAddr), true); err != nil {
				log.Log.Error(err, "failed to write DashboardMetricsAddress")
			}
		}
	}

	// 2. Start the HTTP server when serving the frontend. The component CPU/USS
	// metrics loop runs whenever the metrics registry exists, independent of
	// the frontend flag.
	if h.metrics != nil {
		h.metrics.startComponentMetricsLoop(ctx, h.cfg)
	}
	var httpSrv *HTTPServer
	if h.cfg.ServeFrontend {
		var err error
		httpSrv, err = NewHTTPServer(h.cfg, h.mods, h.metrics)
		if err != nil {
			return err
		}
		go func() { _ = httpSrv.Run(ctx) }()
		host, port := httpSrv.Address()
		log.Log.Info("http server initialized", "address", fmt.Sprintf("%s:%d", host, port))
	} else {
		log.Log.Info("http server disabled.")
	}

	// 3. Publish the dashboard address to GCS KV (KV_NAMESPACE_DASHBOARD).
	// The address carries the http:// scheme so agents can POST /report_events
	// directly (event_agent.py concatenates it with "/report_events").
	// Mirror the Python head (head.py): when the HTTP server is bound to the
	// default 127.0.0.1 the address must stay 127.0.0.1 (agents run on the same
	// node); otherwise expose the node IP for cross-node reporting.
	kvHost := h.cfg.HTTPHost
	if h.cfg.HTTPHost != "127.0.0.1" {
		kvHost = h.cfg.NodeIPAddress
	}
	dashboardAddr := fmt.Sprintf("http://%s:%d", kvHost, h.cfg.HTTPPort)
	if _, err := h.client.InternalKVPut(ctx, "dashboard", "dashboard", []byte(dashboardAddr), true); err != nil {
		log.Log.Error(err, "failed to write DASHBOARD_ADDRESS")
	}

	// 4. Run the GCS liveness probe and every module concurrently. Modules may
	// complete immediately (Start with no background task), so a nil result is
	// only logged and the head keeps running: the gcsCheckAlive probe is the
	// long-lived task that keeps the process resident. Only a real module error
	// or ctx cancellation terminates the head.
	errCh := make(chan error, len(h.mods)+1)
	go func() { errCh <- h.gcsCheckAlive(ctx) }()
	for _, m := range h.mods {
		m := m
		go func() { errCh <- m.Start(ctx) }()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err != nil {
				return err
			}
			// Most modules' Start returns immediately with no background task,
			// so this fires once per module at startup; V(1) keeps it out of
			// the default log while remaining available for debugging.
			log.Log.V(1).Info("dashboard head module finished without error, continuing")
		}
	}
}

// gcsCheckAlive probes GCS liveness every 5s (aligned with the Python
// _gcs_check_alive). Failures are logged but do not terminate the head.
func (h *DashboardHead) gcsCheckAlive(ctx context.Context) error {
	ticker := time.NewTicker(gcsCheckAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := h.client.CheckAlive(ctx, nil); err != nil {
				log.Log.Error(err, "failed to check gcs aliveness")
			}
		}
	}
}
