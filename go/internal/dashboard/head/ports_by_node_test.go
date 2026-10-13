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
	"os"
	"path/filepath"
	"testing"
)

// writePortsByNode writes a ports_by_node.json fixture in dir and returns its
// path. The JSON mirrors python/ray/_private/node.py _get_cached_port: a map
// keyed by Node.unique_id ("{ip}:{plasma_store_socket}") of port-name -> port.
func writePortsByNode(t *testing.T, dir string, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "ports_by_node.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

const validNodeKey = "10.56.181.240:/tmp/ray/session_2026-10-08/sockets/plasma_store"

func TestReadMetricsAgentPortFromCache(t *testing.T) {
	dir := t.TempDir()
	writePortsByNode(t, dir, `{
  "10.56.181.240:/tmp/ray/session_2026-10-08/sockets/plasma_store": {
    "dashboard_agent_listen_port": 52365,
    "metrics_agent_port": 53605,
    "metrics_export_port": 60476
  }
}`)

	port, err := readMetricsAgentPort(dir, "10.56.181.240", "/tmp/ray/session_2026-10-08/sockets/plasma_store")
	if err != nil {
		t.Fatalf("readMetricsAgentPort returned error: %v", err)
	}
	if port != 53605 {
		t.Fatalf("port = %d, want 53605", port)
	}
}

func TestReadMetricsAgentPortMissingFile(t *testing.T) {
	// A session dir without ports_by_node.json must not fail: the driver
	// falls back to the disabled (default) port.
	dir := t.TempDir()
	port, err := readMetricsAgentPort(dir, "10.56.181.240", "/tmp/ray/session/sockets/plasma_store")
	if err != nil {
		t.Fatalf("readMetricsAgentPort with missing file returned error: %v", err)
	}
	if port != 0 {
		t.Fatalf("port = %d, want 0 when file is missing", port)
	}
}

func TestReadMetricsAgentPortUnknownNode(t *testing.T) {
	dir := t.TempDir()
	writePortsByNode(t, dir, `{
  "`+validNodeKey+`": {"metrics_agent_port": 53605}
}`)

	// Same session dir but a different store socket: no entry, returns 0.
	port, err := readMetricsAgentPort(dir, "10.56.181.240", "/tmp/ray/other/sockets/plasma_store")
	if err != nil {
		t.Fatalf("readMetricsAgentPort returned error: %v", err)
	}
	if port != 0 {
		t.Fatalf("port = %d, want 0 for unknown node", port)
	}
}

func TestReadMetricsAgentPortMissingEntry(t *testing.T) {
	dir := t.TempDir()
	writePortsByNode(t, dir, `{
  "`+validNodeKey+`": {"metrics_export_port": 60476}
}`)

	// Node exists but carries no metrics_agent_port entry: returns 0.
	port, err := readMetricsAgentPort(dir, "10.56.181.240", "/tmp/ray/session_2026-10-08/sockets/plasma_store")
	if err != nil {
		t.Fatalf("readMetricsAgentPort returned error: %v", err)
	}
	if port != 0 {
		t.Fatalf("port = %d, want 0 without metrics_agent_port entry", port)
	}
}

func TestReadMetricsAgentPortEmptyArgs(t *testing.T) {
	// Empty session dir, node ip, or store socket means "nothing known",
	// return 0 without touching the filesystem.
	if port, err := readMetricsAgentPort("", "1.2.3.4", "/p"); err != nil || port != 0 {
		t.Fatalf("empty session dir: port = %d, err = %v, want 0/nil", port, err)
	}
	if port, err := readMetricsAgentPort("/p", "", "/s"); err != nil || port != 0 {
		t.Fatalf("empty node ip: port = %d, err = %v, want 0/nil", port, err)
	}
	if port, err := readMetricsAgentPort("/p", "1.2.3.4", ""); err != nil || port != 0 {
		t.Fatalf("empty store socket: port = %d, err = %v, want 0/nil", port, err)
	}
}

func TestReadMetricsAgentPortNonPositiveEntry(t *testing.T) {
	// A cached entry of 0 or negative means "disabled" (mirrors the
	// CoreWorkerOptions default -1); never treat it as a real port.
	dir := t.TempDir()
	writePortsByNode(t, dir, `{
  "`+validNodeKey+`": {"metrics_agent_port": -1}
}`)

	port, err := readMetricsAgentPort(dir, "10.56.181.240", "/tmp/ray/session_2026-10-08/sockets/plasma_store")
	if err != nil {
		t.Fatalf("readMetricsAgentPort returned error: %v", err)
	}
	if port != 0 {
		t.Fatalf("port = %d, want 0 for non-positive cached entry", port)
	}
}
