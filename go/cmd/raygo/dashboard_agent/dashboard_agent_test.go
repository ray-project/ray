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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	dashboardjob "github.com/ray-project/ray/go/internal/dashboard/agent/job"
	dashboardevent "github.com/ray-project/ray/go/internal/dashboard/event"
	"github.com/ray-project/ray/go/internal/dashboard/healthz"
	dashboardlog "github.com/ray-project/ray/go/internal/dashboard/log"
	dashboardreporter "github.com/ray-project/ray/go/internal/dashboard/reporter"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"github.com/spf13/cobra"
)

const (
	// 28-byte (56 hex chars) ids, matching UniqueIDSize.
	testClusterIDHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c"
	testNodeIDHex    = "101112131415161718191a1b1c1d1e1f202122232425262728292a2b"
)

// testArgs returns the required CLI flags with a value override hook.
func testArgs(overrides map[string]string) []string {
	base := map[string]string{
		"node-ip-address":             "1.2.3.4",
		"metrics-export-port":         "44217",
		"grpc-port":                   "8888",
		"listen-port":                 "8080",
		"node-manager-port":           "12345",
		"gcs-address":                 "127.0.0.1:6379",
		"cluster-id-hex":              testClusterIDHex,
		"node-id-hex":                 testNodeIDHex,
		"log-dir":                     "/tmp/ray/logs",
		"temp-dir":                    "/tmp/ray",
		"session-dir":                 "/tmp/ray/session",
		"session-name":                "session_2026_08_24",
		"object-store-name":           "object_store",
		"raylet-name":                 "raylet_1",
		"stdout-filepath":             "/tmp/ray/agent.out",
		"stderr-filepath":             "/tmp/ray/agent.err",
		"logging-level":               "debug",
		"logging-rotate-bytes":        "104857600",
		"logging-rotate-backup-count": "5",
		"disable-metrics-collection":  "true",
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

// TestMetricsExportPortIsOptional verifies the export port flag is optional, so
// a launcher that does not pass it still starts the agent with metrics
// disabled. Making it required would abort every launch from a caller that has
// not been updated, which is the failure this test exists to prevent.
func TestMetricsExportPortIsOptional(t *testing.T) {
	all := testArgs(nil)
	args := make([]string, 0, len(all))
	for i := 0; i < len(all); i++ {
		if all[i] == "--metrics-export-port" {
			i++ // skip the flag and its value
			continue
		}
		args = append(args, all[i])
	}
	// The flag binds a package-level variable, so a previous parse in this
	// process would otherwise leave its value behind.
	metricsExportPort = 0
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags without --metrics-export-port: %v", err)
	}
	cfg, err := buildConfig()
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.MetricsExportPort != 0 {
		t.Errorf("cfg.MetricsExportPort = %d, want 0 when the flag is absent", cfg.MetricsExportPort)
	}
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
	if cfg.MetricsExportPort != 44217 {
		t.Errorf("cfg.MetricsExportPort = %d, want %d", cfg.MetricsExportPort, 44217)
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

func TestBuildConfigInvalidClusterIDHexReportsLength(t *testing.T) {
	args := testArgs(map[string]string{"cluster-id-hex": "nothex"})
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	_, err := buildConfig()
	if err == nil {
		t.Fatal("buildConfig() = nil error, want error for invalid cluster id hex")
	}
	// The error must state the expected and actual length, which is what makes
	// a truncated cluster id diagnosable.
	if !strings.Contains(err.Error(), "expected 56 hex chars, got 6") {
		t.Errorf("error %q does not report expected/actual hex length", err)
	}
}

// TestResolveNodeIDFromEnv covers the Python-parity path: the raylet exports
// the node id in RAY_NODE_ID and the Python agent reads only that variable
// (dashboard/agent.py:70). The Go agent prefers the explicit flag, which the
// raygo launcher always passes.
func TestResolveNodeIDFromEnv(t *testing.T) {
	origNodeIDHex := nodeIDHex
	t.Cleanup(func() { nodeIDHex = origNodeIDHex })

	t.Run("RAY_NODE_ID is used when the flag is empty", func(t *testing.T) {
		t.Setenv("RAY_NODE_ID", testNodeIDHex)
		nodeIDHex = ""
		nodeID, err := resolveNodeID()
		if err != nil {
			t.Fatalf("resolveNodeID: %v", err)
		}
		if nodeID.Hex() != testNodeIDHex {
			t.Errorf("node id = %q, want %q", nodeID.Hex(), testNodeIDHex)
		}
	})

	t.Run("the flag wins over RAY_NODE_ID", func(t *testing.T) {
		t.Setenv("RAY_NODE_ID", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
		nodeIDHex = testNodeIDHex
		nodeID, err := resolveNodeID()
		if err != nil {
			t.Fatalf("resolveNodeID: %v", err)
		}
		if nodeID.Hex() != testNodeIDHex {
			t.Errorf("node id = %q, want the flag value %q", nodeID.Hex(), testNodeIDHex)
		}
	})

	t.Run("neither source set generates a random node id", func(t *testing.T) {
		t.Setenv("RAY_NODE_ID", "")
		nodeIDHex = ""
		nodeID, err := resolveNodeID()
		if err != nil {
			t.Fatalf("resolveNodeID: %v", err)
		}
		if nodeID.Hex() == ids.NilNodeID().Hex() {
			t.Error("resolveNodeID() returned the nil node id, want a freshly generated one")
		}
	})

	t.Run("invalid RAY_NODE_ID names its source", func(t *testing.T) {
		t.Setenv("RAY_NODE_ID", "tooshort")
		nodeIDHex = ""
		_, err := resolveNodeID()
		if err == nil {
			t.Fatal("resolveNodeID() = nil error, want error for invalid RAY_NODE_ID")
		}
		if !strings.Contains(err.Error(), "RAY_NODE_ID") {
			t.Errorf("error %q does not name RAY_NODE_ID as the source", err)
		}
		if !strings.Contains(err.Error(), "expected 56 hex chars, got 8") {
			t.Errorf("error %q does not report expected/actual hex length", err)
		}
	})
}

// TestMinimalModeLoadsNoModules mirrors Python's --minimal behavior: the agent
// skips _init_non_minimal, so no module is loaded or served (agent.py:94-95).
func TestMinimalModeLoadsNoModules(t *testing.T) {
	mods, err := buildModules(agent.Config{Minimal: true, LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("buildModules(minimal): %v", err)
	}
	if len(mods) != 0 {
		t.Errorf("buildModules(minimal) returned %d modules, want 0", len(mods))
	}
}

// TestRunDashboardAgentMinimalSkipsModules verifies the --minimal flag reaches
// the agent config and that no module is handed to the runner, matching the
// Python agent's _init_non_minimal skip.
func TestRunDashboardAgentMinimalSkipsModules(t *testing.T) {
	origMinimal := minimal
	origIsHead := isHeadFlag
	t.Cleanup(func() {
		minimal = origMinimal
		isHeadFlag = origIsHead
	})

	args := append(testArgs(map[string]string{"log-dir": t.TempDir(), "is-head": "true"}), "--minimal")
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	origLoad := loadGCSClientFn
	loadGCSClientFn = func(gcs.ClientOptions) (gcs.Client, error) {
		return &rejectGetAllGCS{t: t}, nil
	}
	defer func() { loadGCSClientFn = origLoad }()

	origValidate := validatePluginLoadableFn
	validatePluginLoadableFn = func() error { return nil }
	defer func() { validatePluginLoadableFn = origValidate }()

	var got agent.Config
	var gotMods []agent.Module
	origRun := runAgentFn
	runAgentFn = func(_ context.Context, cfg agent.Config, mods []agent.Module) error {
		got = cfg
		gotMods = mods
		return nil
	}
	defer func() { runAgentFn = origRun }()

	if err := runDashboardAgent(context.Background()); err != nil {
		t.Fatalf("runDashboardAgent: %v", err)
	}
	if !got.Minimal {
		t.Error("cfg.Minimal = false, want true for --minimal")
	}
	if len(gotMods) != 0 {
		t.Errorf("runDashboardAgent passed %d modules in minimal mode, want 0", len(gotMods))
	}
}

// TestListenPortDefaultsToAgentPort verifies the HTTP listen port is optional
// and defaults to the shared dashboard agent port, so a launcher that omits
// the flag still starts the agent on the port the head side expects.
func TestListenPortDefaultsToAgentPort(t *testing.T) {
	flag := DashboardAgentCmd.Flags().Lookup("listen-port")
	if flag == nil {
		t.Fatal("flag \"listen-port\" is not registered")
	}
	want := strconv.Itoa(common.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
	if flag.DefValue != want {
		t.Errorf("--listen-port default = %q, want the shared agent port %q",
			flag.DefValue, want)
	}
}

// TestFlagRequiredness pins which flags the raygo launcher must pass. The
// log-rotation limits are required because the agent always redirects its
// logs; the session name and the stdout/stderr targets are optional so a
// caller that does not redirect output keeps working.
func TestFlagRequiredness(t *testing.T) {
	required := []string{
		"node-ip-address",
		"grpc-port",
		"node-manager-port",
		"gcs-address",
		"cluster-id-hex",
		"log-dir",
		"temp-dir",
		"session-dir",
		"object-store-name",
		"raylet-name",
		"logging-rotate-bytes",
		"logging-rotate-backup-count",
	}
	optional := []string{
		"listen-port",
		"session-name",
		"stdout-filepath",
		"stderr-filepath",
		"metrics-export-port",
	}
	for _, name := range required {
		flag := DashboardAgentCmd.Flags().Lookup(name)
		if flag == nil {
			t.Fatalf("flag %q is not registered", name)
		}
		if _, ok := flag.Annotations[cobra.BashCompOneRequiredFlag]; !ok {
			t.Errorf("flag %q is not marked required", name)
		}
	}
	for _, name := range optional {
		flag := DashboardAgentCmd.Flags().Lookup(name)
		if flag == nil {
			t.Fatalf("flag %q is not registered", name)
		}
		if _, ok := flag.Annotations[cobra.BashCompOneRequiredFlag]; ok {
			t.Errorf("flag %q is marked required, want optional", name)
		}
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

// TestBuildModules verifies the log, healthz and event modules are wired into
// the agent module list and that a missing log directory fails startup before
// serving.
func TestBuildModules(t *testing.T) {
	fakeGCS := &buildModulesFakeGCS{}
	t.Run("returns log, healthz, event, reporter and job modules", func(t *testing.T) {
		mods, err := buildModules(agent.Config{LogDir: t.TempDir(), GCS: fakeGCS})
		if err != nil {
			t.Fatalf("buildModules: %v", err)
		}
		if len(mods) != 5 {
			t.Fatalf("buildModules returned %d modules, want 5", len(mods))
		}
		if _, ok := mods[0].(*dashboardlog.LogAgentModule); !ok {
			t.Errorf("module[0] type = %T, want *log.LogAgentModule", mods[0])
		}
		if _, ok := mods[1].(*healthz.HealthzAgent); !ok {
			t.Errorf("module[1] type = %T, want *healthz.HealthzAgent", mods[1])
		}
		if _, ok := mods[2].(*dashboardevent.EventAgent); !ok {
			t.Errorf("module[2] type = %T, want *event.EventAgent", mods[2])
		}
		if _, ok := mods[3].(*dashboardreporter.ReporterModule); !ok {
			t.Errorf("module[3] type = %T, want *reporter.ReporterModule", mods[3])
		}
		if _, ok := mods[4].(*dashboardjob.JobAgent); !ok {
			t.Errorf("module[4] type = %T, want *job.JobAgent", mods[4])
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

// TestIsHeadRetryDelayIsJittered verifies the retry backoff is randomized
// within [0.8, 1.2] of the base interval so co-launched agents do not poll GCS
// in lockstep.
func TestIsHeadRetryDelayIsJittered(t *testing.T) {
	const interval = 100 * time.Millisecond
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		delay := isHeadRetryDelay(interval)
		if delay < 80*time.Millisecond || delay > 120*time.Millisecond {
			t.Fatalf("isHeadRetryDelay(%v) = %v, want a delay within [80ms, 120ms]", interval, delay)
		}
		seen[delay] = true
	}
	if len(seen) == 1 {
		t.Error("isHeadRetryDelay returned the same delay every time, want a randomized backoff")
	}
}

func TestRunDashboardAgentConnectsThroughPluginSeam(t *testing.T) {
	args := testArgs(map[string]string{"log-dir": t.TempDir()})
	if err := DashboardAgentCmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	orig := loadGCSClientFn
	loadGCSClientFn = func(opts gcs.ClientOptions) (gcs.Client, error) {
		return nil, errors.New("plugin connect failed")
	}
	defer func() { loadGCSClientFn = orig }()

	origValidate := validatePluginLoadableFn
	validatePluginLoadableFn = func() error { return nil }
	defer func() { validatePluginLoadableFn = origValidate }()

	err := runDashboardAgent(context.Background())
	if err == nil {
		t.Fatal("runDashboardAgent() = nil error, want connect error through the seam")
	}
	if !strings.Contains(err.Error(), "failed to connect to GCS client") {
		t.Fatalf("error %q does not mention failed to connect to GCS client", err)
	}
}

// rejectGetAllGCS fails the test when the node table is queried, proving that
// an explicit --is-head value short-circuits GCS derivation. Deriving is_head
// from GCS on the raylet-launched path would deadlock: raylet starts the agent
// before registering its own node, so the agent would poll for a node that only
// appears once the agent is ready.
type rejectGetAllGCS struct {
	gcs.Client
	t *testing.T
}

func (f *rejectGetAllGCS) GetAll(_ context.Context, _ []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	f.t.Error("GetAll called: an explicit --is-head must skip GCS derivation")
	return nil, errors.New("GetAll must not be called")
}

// Close is a no-op so runDashboardAgent's deferred Close does not dispatch to
// the nil embedded gcs.Client.
func (f *rejectGetAllGCS) Close() error { return nil }

// TestRunDashboardAgentExplicitIsHeadSkipsGCS verifies that an explicit
// --is-head value is used as-is and that GCS is never consulted, breaking the
// startup deadlock where raylet waits for the agent while the agent waits for
// raylet to register the node in GCS.
func TestRunDashboardAgentExplicitIsHeadSkipsGCS(t *testing.T) {
	origIsHead := isHeadFlag
	defer func() { isHeadFlag = origIsHead }()

	for _, tc := range []struct {
		flag string
		want bool
	}{
		{flag: "true", want: true},
		{flag: "false", want: false},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			args := testArgs(map[string]string{
				"log-dir": t.TempDir(),
				"is-head": tc.flag,
			})
			if err := DashboardAgentCmd.ParseFlags(args); err != nil {
				t.Fatalf("parse flags: %v", err)
			}

			origLoad := loadGCSClientFn
			loadGCSClientFn = func(gcs.ClientOptions) (gcs.Client, error) {
				return &rejectGetAllGCS{t: t}, nil
			}
			defer func() { loadGCSClientFn = origLoad }()

			origValidate := validatePluginLoadableFn
			validatePluginLoadableFn = func() error { return nil }
			defer func() { validatePluginLoadableFn = origValidate }()

			var got agent.Config
			origRun := runAgentFn
			runAgentFn = func(_ context.Context, cfg agent.Config, _ []agent.Module) error {
				got = cfg
				return nil
			}
			defer func() { runAgentFn = origRun }()

			if err := runDashboardAgent(context.Background()); err != nil {
				t.Fatalf("runDashboardAgent: %v", err)
			}
			if got.IsHead != tc.want {
				t.Errorf("cfg.IsHead = %v, want %v", got.IsHead, tc.want)
			}
		})
	}
}
