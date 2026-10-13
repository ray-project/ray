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

package head

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// readMetricsAgentPort returns the cached metrics agent port for this node from
// session_dir/ports_by_node.json. The file is written by the dashboard agent at
// ray start (python/ray/_private/node.py), which caches the node-local ports so
// different driver processes on the same node reuse the same metrics port
// instead of each connecting to a different exporter address. A driver
// (e.g. the dashboard head's CoreWorker) should reuse that port so its
// exporter can talk to the dashboard agent's grpc server.
//
// The JSON is keyed by Node.unique_id, "{node_ip_address}:{plasma_store_socket}",
// and maps port names to ports. Returns 0 when the port is unavailable (file
// missing, node key unknown, or no metrics_agent_port entry), so callers fall
// back to the default (disabled) port. Any other read/parse error is returned
// so the caller can log it; metrics setup is best-effort and must not fail the
// caller.
func readMetricsAgentPort(sessionDir, nodeIP, storeSocket string) (int, error) {
	if sessionDir == "" || nodeIP == "" || storeSocket == "" {
		return 0, nil
	}
	data, err := os.ReadFile(filepath.Join(sessionDir, "ports_by_node.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	// map[string]map[string]int: {node unique id: {port name: port}}.
	var portsByNode map[string]map[string]int
	if err := json.Unmarshal(data, &portsByNode); err != nil {
		return 0, err
	}
	entry := portsByNode[nodeIP+":"+storeSocket]
	if entry == nil {
		return 0, nil
	}
	if port, ok := entry["metrics_agent_port"]; ok && port > 0 {
		return port, nil
	}
	return 0, nil
}
