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

package agent

import (
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ray-project/ray/go/pkg/ids"
)

// Port-discovery file names, mirroring src/ray/common/constants.h. raylet's
// WaitForDashboardAgentPorts waits on these files when the corresponding
// configured port is 0 (dynamic allocation).
const (
	metricsAgentPortName         = "metrics_agent_port"
	metricsExportPortName        = "metrics_export_port"
	dashboardAgentListenPortName = "dashboard_agent_listen_port"
)

// persistAgentPorts writes the two server port-discovery files raylet waits for
// before it registers this node in GCS. Both are the ports the agent actually
// bound, which differ from the configured 0 when dynamically allocated. The
// metrics export port is written separately by persistMetricsExportPort: it is
// only known once the module that serves it has bound, and each file is
// written exactly once because raylet reads a port file as soon as it appears
// and never re-reads it.
//
// An empty SessionDir means there is nowhere to publish (tests that exercise
// Run without a real session directory); persistence is skipped in that case.
func persistAgentPorts(cfg Config, metricsAgentPort, listenPort int) error {
	if cfg.SessionDir == "" {
		return nil
	}
	ports := []struct {
		name string
		port int
	}{
		{metricsAgentPortName, metricsAgentPort},
		{dashboardAgentListenPortName, listenPort},
	}
	for _, p := range ports {
		if err := persistPort(cfg.SessionDir, cfg.NodeID, p.name, p.port); err != nil {
			return err
		}
	}
	return nil
}

// persistMetricsExportPort writes the metrics export port file once the module
// that serves it has bound, so the published value is the port something
// actually answers on rather than the port the agent was asked for. A
// non-positive port, or a disabled collection, publishes the not-in-use
// sentinel. The file is written once: raylet reads a port file when it appears
// and never re-reads it, so a placeholder followed by a rewrite would let the
// poller latch onto the placeholder.
func persistMetricsExportPort(cfg Config, port int) error {
	if cfg.SessionDir == "" {
		return nil
	}
	if cfg.MetricsCollectionDisabled || port <= 0 {
		port = -1
	}
	return persistPort(cfg.SessionDir, cfg.NodeID, metricsExportPortName, port)
}

// persistPort writes one port file at "{dir}/{port_name}_{node_id_hex}",
// mirroring ray::PersistPort.
func persistPort(dir string, nodeID ids.NodeID, portName string, port int) error {
	path := filepath.Join(dir, portName+"_"+nodeID.Hex())
	return writeFileAtomic(path, strconv.Itoa(port))
}

// writeFileAtomic mirrors ray::WriteFile: it writes "<path>.tmp" and renames it
// over the destination, so a concurrent reader (raylet) never observes a
// partially written port. os.Rename replaces the destination atomically on
// POSIX, so no explicit remove is needed.
func writeFileAtomic(path, value string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(value), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// listenerPort returns the TCP port a bound listener is using, which is the
// actual ephemeral port when it was bound to port 0.
func listenerPort(lis net.Listener) int {
	if addr, ok := lis.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}
	return 0
}
