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

// Package insight implements the InsightHead dashboard module: a pure HTTP
// reverse proxy to the flow insight monitor service, aligned with the Python
// InsightHead (python/ray/dashboard/modules/insight/insight_head.py).
package insight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
)

// Insight KV constants, aligned with insight_head.py: the monitor actor writes
// "insight_monitor_address" under the "flowinsight" namespace, with the value
// "{node_ip}:{port}".
const (
	insightKVNamespace = "flowinsight"
	insightKVKey       = "insight_monitor_address"
)

// httpClient is the outbound client used for the health check ping and the
// proxy round trip.
var httpClient = &http.Client{Timeout: 10 * time.Second}

// InsightHead proxies /insight/{path:.*} to the flow insight monitor service.
type InsightHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	proxy  *httputil.ReverseProxy
}

// New creates an InsightHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *InsightHead {
	// The target is fixed per request from the KV-discovered address, so the
	// ReverseProxy rewrite is a no-op aside from rewriting the request host.
	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Log.Error(err, "error proxying request to insight monitor", "path", r.URL.Path)
			// data is {"success": false}, aligned with the Python
			// rest_response(success=False) on the insight proxy error path.
			head.RESTResponseCamel(w, http.StatusInternalServerError,
				fmt.Sprintf("Error proxying request to insight monitor: %v", err),
				map[string]interface{}{"success": false})
		},
	}
	return &InsightHead{cfg: cfg, client: client, proxy: proxy}
}

// Name returns the module name.
func (h *InsightHead) Name() string { return "InsightHead" }

// Start has no background tasks; the KV address is read per request.
func (h *InsightHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (h *InsightHead) Healthy() bool { return true }

// RegisterHTTP registers the /insight/{path...} proxy routes for GET and POST,
// aligned with the Python routes in insight_head.py. The trailing {path...}
// wildcard is the Go ServeMux equivalent of the Python {path:.*} and matches
// the remainder of the path including slashes.
func (h *InsightHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /insight/{path...}", h.handle)
	mux.HandleFunc("POST /insight/{path...}", h.handle)
	return nil
}

// handle proxies a single request to the insight monitor service. The request
// method is distinguished by the registered route; only GET and POST reach
// this handler (aligned with Python, which returns 500 for any other method).
func (h *InsightHead) handle(w http.ResponseWriter, r *http.Request) {
	address, err := h.insightAddress(r.Context())
	if err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"success": false})
		return
	}

	// Health check: ping the insight service and require result == true
	// (aligned with is_insight_server_alive, which returns False when the
	// address is empty or the ping result is falsy).
	if !h.insightServerAlive(r.Context(), address) {
		// The cached address is stale; re-read the KV. Still missing (or the
		// address changed) -> 500, otherwise proxy with the fresh address.
		address, err = h.insightAddress(r.Context())
		if err != nil {
			head.RESTResponseCamel(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"success": false})
			return
		}
	}

	// Build the target URL: http://{address}/{path} + optional query string.
	target, err := h.targetURL(address, r)
	if err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"success": false})
		return
	}

	// Proxy the request, preserving the original headers and body. The
	// ReverseProxy copies the inbound headers/body and streams back the target
	// status, headers and body unchanged.
	r.URL = target
	r.Host = target.Host
	h.proxy.ServeHTTP(w, r)
}

// insightAddress reads the insight monitor address from the GCS KV store. It
// returns an error carrying the Python-aligned "address not found" message when
// the key is missing.
func (h *InsightHead) insightAddress(ctx context.Context) (string, error) {
	reply, err := h.client.InternalKVGet(ctx, insightKVNamespace, insightKVKey)
	if err != nil {
		return "", fmt.Errorf("failed to read insight monitor address from KV store: %v", err)
	}
	if reply == nil || len(reply.Value) == 0 {
		return "", fmt.Errorf("InsightMonitor address not found in KV store")
	}
	return string(reply.Value), nil
}

// insightServerAlive pings the insight monitor service and reports whether it
// is alive (resp["result"] is truthy), aligned with the Python
// is_insight_server_alive / async_ping.
func (h *InsightHead) insightServerAlive(ctx context.Context, address string) bool {
	pingURL := fmt.Sprintf("http://%s/ping", address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL, nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var body struct {
		Result bool `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return false
	}
	return body.Result
}

// targetURL builds the target URL for the proxy request: the KV address, the
// wildcard path and the original query string.
func (h *InsightHead) targetURL(address string, r *http.Request) (*url.URL, error) {
	path := r.PathValue("path")
	target := &url.URL{
		Scheme: "http",
		Host:   address,
		Path:   "/" + path,
	}
	if q := r.URL.RawQuery; q != "" {
		target.RawQuery = q
	}
	return target, nil
}
