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

package dashboard_agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/internal/dashboard/healthz"
	dashboardlog "github.com/ray-project/ray/go/internal/dashboard/log"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

const (
	// 28-byte (56 hex chars) ids, matching UniqueIDSize.
	testClusterIDHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c"
	testNodeIDHex    = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b"
)

// testArgs returns the required CLI flags with a value override hook.
func testArgs(overrides map[string]string) []string {
	base := map[string]string{
		"node-ip-address":            "1.2.3.4",
		"grpc-port":                  "8888",
		"listen-port":                "8080",
		"node-manager-port":          "12345",
		"gcs-address":                "127.0.0.1:6379",
		"cluster-id-hex":             testClusterIDHex,
		"node-id-hex":                testNodeIDHex,
		"log-dir":                    "/tmp/ray/logs",
		"temp-dir":                   "/tmp/ray",
		"session-dir":                "/tmp/ray/session",
		"session-name":               "session_2026_08_24",
		"object-store-name":          "object_store",
		"raylet-name":                "raylet_1",
		"stdout-filepath":            "/tmp/ray/agent.out",
		"stderr-filepath":            "/tmp/ray/agent.err",
		"logging-level":              "debug",
		"disable-metrics-collection": "true",
		"events-export-addr":         "localhost:8125",
	}
	for k, v := range overrides {
		base[k] = v
	}
	args := make([]string, 0, len(base)*2)
	for k, v := range base {
		args = append(args, "--"+k, v)
	}
	return args
}

func TestBuildConfig(t *testing.T) {
	if err := DashboardAgentCmd.ParseFlags(testArgs(nil)); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg, err := buildConfig()
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}

	if cfg.NodeIP != "1.2.3.4" {
		t.Errorf("cfg.NodeIP = %q, want %q", cfg.NodeIP, "1.2.3.4")
	}
	if cfg.GRPCPort != 8888 {
		t.Errorf("cfg.GRPCPort = %d, want %d", cfg.GRPCPort, 8888)
	}
	if cfg.ListenPort != 8080 {
		t.Errorf("cfg.ListenPort = %d, want %d", cfg.ListenPort, 8080)
	}
	if cfg.NodeManagerPort != 12345 {
		t.Errorf("cfg.NodeManagerPort = %d, want %d", cfg.NodeManagerPort, 12345)
	}
	if cfg.GCSAddress != "127.0.0.1:6379" {
		t.Errorf("cfg.GCSAddress = %q, want %q", cfg.GCSAddress, "127.0.0.1:6379")
	}
	if cfg.ClusterID.Hex() != testClusterIDHex {
		t.Errorf("cfg.ClusterID.Hex() = %q, want %q", cfg.ClusterID.Hex(), testClusterIDHex)
	}
	if cfg.NodeID.Hex() != testNodeIDHex {
		t.Errorf("cfg.NodeID.Hex() = %q, want %q", cfg.NodeID.Hex(), testNodeIDHex)
	}
	if cfg.LogDir != "/tmp/ray/logs" {
		t.Errorf("cfg.LogDir = %q, want %q", cfg.LogDir, "/tmp/ray/logs")
	}
	if cfg.TempDir != "/tmp/ray" {
		t.Errorf("cfg.TempDir = %q, want %q", cfg.TempDir, "/tmp/ray")
	}
	if cfg.SessionDir != "/tmp/ray/session" {
		t.Errorf("cfg.SessionDir = %q, want %q", cfg.SessionDir, "/tmp/ray/session")
	}
	if cfg.SessionName != "session_2026_08_24" {
		t.Errorf("cfg.SessionName = %q, want %q", cfg.SessionName, "session_2026_08_24")
	}
	if !cfg.MetricsCollectionDisabled {
		t.Errorf("cfg.MetricsCollectionDisabled = false, want true")
	}
	if cfg.EventsExportAddr != "localhost:8125" {
		t.Errorf("cfg.EventsExportAddr = %q, want %q", cfg.EventsExportAddr, "localhost:8125")
	}
}

// TestBuildConfigToleratesNodeManagerPlaceholder verifies that the agent
// accepts the literal RAY_NODE_MANAGER_PORT_PLACEHOLDER value raylet passes
// in --dashboard_agent_command, mirroring the Python agent which reads it as
// an untyped string. The agent never dials the node manager, so the value is
// tolerated and collapsed to a zero port rather than rejected at parse time.
func TestBuildConfigToleratesNodeManagerPlaceholder(t *testing.T) {
	args := testArgs(map[string]string{
		"node-manager-port": "RAY_NODE_MANAGER_PORT_PLACEHOLDER",
	})
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	cfg, err := buildConfig()
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.NodeManagerPort != 0 {
		t.Errorf("cfg.NodeManagerPort = %d, want 0 for placeholder", cfg.NodeManagerPort)
	}
}

func TestBuildConfigInvalidNodeIDHex(t *testing.T) {
	args := testArgs(map[string]string{"node-id-hex": "zz-not-hex"})
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if _, err := buildConfig(); err == nil {
		t.Fatal("buildConfig() = nil error, want error for invalid node id hex")
	} else if !strings.Contains(err.Error(), "node id") {
		t.Errorf("error %q does not mention node id", err)
	}
}

func TestPreRunERejectsBadLoggingLevel(t *testing.T) {
	args := testArgs(map[string]string{"logging-level": "bogus"})
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	err := DashboardAgentCmd.PreRunE(DashboardAgentCmd, args)
	if err == nil {
		t.Fatal("PreRunE() = nil error, want error for invalid logging-level")
	}
	if !strings.Contains(err.Error(), "logging-level") {
		t.Errorf("error %q does not mention logging-level", err)
	}
}

// buildModulesFakeGCS stubs the GCS client so healthz.New accepts the config
// without dialing a real GCS (the healthz module never calls CheckAlive during
// construction).
type buildModulesFakeGCS struct {
	gcs.Client
	isHead bool
	err    error
	// failFirst makes the first N GetAll calls return an error before the
	// normal behavior kicks in, exercising the is_head retry loop.
	failFirst int
	calls     int
}

// GetAll fakes the node table lookup used by resolveIsHead: it returns a fake
// node info for every requested id, carrying the configured is_head flag.
func (f *buildModulesFakeGCS) GetAll(_ context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	if f.failFirst > 0 && f.calls < f.failFirst {
		f.calls++
		return nil, errors.New("gcs unreachable")
	}
	if f.err != nil {
		return nil, f.err
	}
	result := make(map[ids.NodeID]*proto.GcsNodeInfo, len(nodeIDs))
	for _, id := range nodeIDs {
		result[id] = &proto.GcsNodeInfo{IsHeadNode: f.isHead}
	}
	return result, nil
}

// TestBuildModules verifies the log and healthz modules are wired into the
// agent module list and that a missing log directory fails startup before
// serving.
func TestBuildModules(t *testing.T) {
	fakeGCS := &buildModulesFakeGCS{}
	t.Run("returns log and healthz modules", func(t *testing.T) {
		mods, err := buildModules(agent.Config{LogDir: t.TempDir(), GCS: fakeGCS})
		if err != nil {
			t.Fatalf("buildModules: %v", err)
		}
		if len(mods) != 2 {
			t.Fatalf("buildModules returned %d modules, want 2", len(mods))
		}
		if _, ok := mods[0].(*dashboardlog.LogAgentModule); !ok {
			t.Errorf("module[0] type = %T, want *log.LogAgentModule", mods[0])
		}
		if _, ok := mods[1].(*healthz.HealthzAgent); !ok {
			t.Errorf("module[1] type = %T, want *healthz.HealthzAgent", mods[1])
		}
	})

	t.Run("missing log dir fails", func(t *testing.T) {
		if _, err := buildModules(agent.Config{LogDir: t.TempDir() + "/missing", GCS: fakeGCS}); err == nil {
			t.Fatal("buildModules() = nil error, want error for missing log dir")
		}
	})
}

func TestResolveIsHead(t *testing.T) {
	ctx := context.Background()
	nodeID := ids.NewNodeID()
	nilNodeID := ids.NilNodeID()

	t.Run("head returns true", func(t *testing.T) {
		fake := &buildModulesFakeGCS{isHead: true}
		got, err := resolveIsHead(ctx, fake, nodeID)
		if err != nil {
			t.Fatalf("resolveIsHead: %v", err)
		}
		if !got {
			t.Error("resolveIsHead = false, want true for a head node")
		}
	})

	t.Run("worker returns false", func(t *testing.T) {
		fake := &buildModulesFakeGCS{isHead: false}
		got, err := resolveIsHead(ctx, fake, nodeID)
		if err != nil {
			t.Fatalf("resolveIsHead: %v", err)
		}
		if got {
			t.Error("resolveIsHead = true, want false for a worker node")
		}
	})

	t.Run("nil node id returns false", func(t *testing.T) {
		fake := &buildModulesFakeGCS{isHead: true}
		got, err := resolveIsHead(ctx, fake, nilNodeID)
		if err != nil {
			t.Fatalf("resolveIsHead(nil node id): %v", err)
		}
		if got {
			t.Error("resolveIsHead(nil node id) = true, want false")
		}
	})

	t.Run("gcs error is propagated after retries", func(t *testing.T) {
		fake := &buildModulesFakeGCS{err: errors.New("gcs unreachable")}
		if _, err := resolveIsHeadRetry(ctx, fake, nodeID, 2, time.Millisecond); err == nil {
			t.Fatal("resolveIsHeadRetry = nil error, want error when GCS lookup keeps failing")
		}
	})

	t.Run("recovers from transient gcs failures", func(t *testing.T) {
		// Mirrors the startup race the Python agent tolerates with
		// call_with_retry: the node may not be in the table yet.
		fake := &buildModulesFakeGCS{isHead: true, failFirst: 3}
		got, err := resolveIsHeadRetry(ctx, fake, nodeID, 5, time.Millisecond)
		if err != nil {
			t.Fatalf("resolveIsHeadRetry: %v", err)
		}
		if !got {
			t.Error("resolveIsHeadRetry = false, want true once the lookup succeeds")
		}
	})
}
