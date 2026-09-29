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

package reporter

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/proto"
)

// handlePrometheusSD serves /api/prometheus/sd. It builds the file-based
// service discovery content aligned with
// PrometheusServiceDiscoveryWriter.get_file_discovery_content in
// python/ray/_private/metrics_agent.py: the alive nodes' metrics export
// addresses plus the autoscaler monitor and dashboard metrics addresses, all
// labeled with {"job": "ray"}.
func (r *ReportHead) handlePrometheusSD(w http.ResponseWriter, req *http.Request) {
	targets, err := r.serviceDiscoveryTargets(req)
	if err != nil {
		http.Error(w, `{"error": "service discovery error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(targets)
}

// serviceDiscoveryTargets computes the [{labels,targets}] list. The node
// information comes from a live GCS node pull (the Go dashboard has no
// background metrics_agent thread).
func (r *ReportHead) serviceDiscoveryTargets(req *http.Request) ([]byte, error) {
	var addrs []string

	nodeReply, err := r.client.GetAllNodeInfo(req.Context(), &proto.GetAllNodeInfoRequest{})
	if err != nil {
		return nil, err
	}
	for _, n := range nodeReply.NodeInfoList {
		if n.State != proto.GcsNodeInfo_DEAD {
			addrs = append(addrs, buildAddress(n.NodeManagerAddress, int(n.MetricsExportPort)))
		}
	}
	if reply, err := r.client.InternalKVGet(req.Context(), "", autoscalerMetricsAddress); err == nil && len(reply.Value) > 0 {
		addrs = append(addrs, string(reply.Value))
	}
	if reply, err := r.client.InternalKVGet(req.Context(), "", dashboardMetricsAddress); err == nil && len(reply.Value) > 0 {
		addrs = append(addrs, string(reply.Value))
	}

	// Preserve the append order (alive nodes in node-table order, then the
	// autoscaler and dashboard addresses), aligned with
	// PrometheusServiceDiscoveryWriter.get_file_discovery_content which appends
	// in the same sequence without sorting.
	content := []map[string]interface{}{
		{"labels": map[string]string{"job": "ray"}, "targets": addrs},
	}
	return json.Marshal(content)
}

// buildAddress ports BuildAddress from src/ray/util/network_util.cc: IPv6
// hosts are wrapped in square brackets.
func buildAddress(host string, port int) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + strconv.Itoa(port)
	}
	return host + ":" + strconv.Itoa(port)
}
