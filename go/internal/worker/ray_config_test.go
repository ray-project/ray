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

package worker

import (
	"os"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
	contract "github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/spf13/cobra"
)

func TestActorLifetime_String(t *testing.T) {
	tests := []struct {
		name     string
		lifetime ActorLifetime
		expected string
	}{
		{name: "Detached", lifetime: Detached, expected: "DETACHED"},
		{name: "NonDetached", lifetime: NonDetached, expected: "NON_DETACHED"},
		{name: "Unknown", lifetime: ActorLifetime(999), expected: "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lifetime.String(); got != tt.expected {
				t.Errorf("ActorLifetime(%d).String() = %q, want %q", tt.lifetime, got, tt.expected)
			}
		})
	}
}

func TestRayConfig_SetBootstrapAddress(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		wantIP    string
		wantPort  int
		expectErr bool
	}{
		{name: "Valid address", address: "192.168.1.100:6379", wantIP: "192.168.1.100", wantPort: 6379},
		{name: "Different port", address: "10.0.0.1:8080", wantIP: "10.0.0.1", wantPort: 8080},
		{name: "Invalid address", address: "invalid-address", wantIP: "", wantPort: 0, expectErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RayConfig{}
			err := config.SetBootstrapAddress(tt.address)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for address %q, got nil", tt.address)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error for address %q: %v", tt.address, err)
			}
			if config.BootstrapIP != tt.wantIP {
				t.Errorf("BootstrapIP = %q, want %q", config.BootstrapIP, tt.wantIP)
			}
			if config.BootstrapPort != tt.wantPort {
				t.Errorf("BootstrapPort = %d, want %d", config.BootstrapPort, tt.wantPort)
			}
		})
	}
}

func TestRayConfig_GetBootstrapAddress(t *testing.T) {
	tests := []struct {
		name          string
		bootstrapIP   string
		bootstrapPort int
		expected      string
	}{
		{name: "Valid address", bootstrapIP: "192.168.1.100", bootstrapPort: 6379, expected: "192.168.1.100:6379"},
		{name: "Empty IP", bootstrapIP: "", bootstrapPort: 6379, expected: ""},
		{name: "Different port", bootstrapIP: "10.0.0.1", bootstrapPort: 8080, expected: "10.0.0.1:8080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RayConfig{BootstrapIP: tt.bootstrapIP, BootstrapPort: tt.bootstrapPort}
			if got := config.GetBootstrapAddress(); got != tt.expected {
				t.Errorf("GetBootstrapAddress() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestRayConfig_UpdateSessionDir(t *testing.T) {
	tests := []struct {
		name            string
		initialSession  string
		initialLogs     string
		inputDir        string
		expectedSession string
		expectedLogs    string
	}{
		{name: "Empty session and logs", inputDir: "/tmp/ray", expectedSession: "/tmp/ray", expectedLogs: "/tmp/ray/logs"},
		{name: "Existing session dir", initialSession: "/custom/session", inputDir: "/tmp/ray", expectedSession: "/custom/session", expectedLogs: "/tmp/ray/logs"},
		{name: "Existing logs dir", initialLogs: "/custom/logs", inputDir: "/tmp/ray", expectedSession: "/tmp/ray", expectedLogs: "/custom/logs"},
		{name: "Both existing", initialSession: "/custom/session", initialLogs: "/custom/logs", inputDir: "/tmp/ray", expectedSession: "/custom/session", expectedLogs: "/custom/logs"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RayConfig{SessionDir: tt.initialSession, LogsDir: tt.initialLogs}
			config.UpdateSessionDir(tt.inputDir)
			if config.SessionDir != tt.expectedSession {
				t.Errorf("SessionDir = %q, want %q", config.SessionDir, tt.expectedSession)
			}
			if config.LogsDir != tt.expectedLogs {
				t.Errorf("LogsDir = %q, want %q", config.LogsDir, tt.expectedLogs)
			}
		})
	}
}

func TestParseDefaultActorLifetimeType(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  ActorLifetime
		expectErr bool
	}{
		{name: "detached lowercase", input: "detached", expected: Detached},
		{name: "DETACHED uppercase", input: "DETACHED", expected: Detached},
		{name: "non_detached lowercase", input: "non_detached", expected: NonDetached},
		{name: "NON_DETACHED uppercase", input: "NON_DETACHED", expected: NonDetached},
		{name: "Mixed case detached", input: "DeTaChEd", expected: Detached},
		{name: "Invalid value", input: "invalid", expected: NonDetached, expectErr: true},
		{name: "Empty string", input: "", expected: NonDetached, expectErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseDefaultActorLifetimeType(tt.input)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for input %q, got nil", tt.input)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error for input %q: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("ParseDefaultActorLifetimeType(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDeserialize(t *testing.T) {
	tests := []struct {
		name      string
		jsonStr   string
		expectNil bool
		expectErr bool
	}{
		{name: "Empty string", jsonStr: "", expectNil: true},
		{name: "Valid JSON object", jsonStr: `{"pip": ["requests"], "env_vars": {"KEY": "value"}}`},
		{name: "Valid empty JSON object", jsonStr: `{}`},
		{name: "Valid JSON array", jsonStr: `[]`},
		{name: "Invalid JSON", jsonStr: `{invalid json}`, expectNil: true, expectErr: true},
		{name: "Malformed JSON", jsonStr: `{"key": "value"`, expectNil: true, expectErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Deserialize(tt.jsonStr)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for JSON %q, got nil", tt.jsonStr)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error for JSON %q: %v", tt.jsonStr, err)
			}
			if tt.expectNil && result != nil {
				t.Errorf("expected nil result for JSON %q, got %v", tt.jsonStr, result)
			}
			if !tt.expectNil && result == nil {
				t.Fatalf("expected non-nil result for valid JSON %q", tt.jsonStr)
			}
			if result != nil && result.RawJSON != tt.jsonStr {
				t.Errorf("Deserialize(%q).RawJSON = %q, want %q", tt.jsonStr, result.RawJSON, tt.jsonStr)
			}
		})
	}
}

func TestRayConfig_IsWorker_IsDriver(t *testing.T) {
	tests := []struct {
		name       string
		workerType options.WorkerType
		wantWorker bool
		wantDriver bool
	}{
		{name: "Worker type", workerType: options.WorkerTypeWorker, wantWorker: true},
		{name: "Driver type", workerType: options.WorkerTypeDriver, wantDriver: true},
		{name: "Unknown type", workerType: options.WorkerType(999)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RayConfig{WorkerType: tt.workerType}
			if got := config.IsWorker(); got != tt.wantWorker {
				t.Errorf("IsWorker() = %v, want %v", got, tt.wantWorker)
			}
			if got := config.IsDriver(); got != tt.wantDriver {
				t.Errorf("IsDriver() = %v, want %v", got, tt.wantDriver)
			}
		})
	}
}

func TestRayConfig_IsClusterMode_IsLocalMode(t *testing.T) {
	tests := []struct {
		name        string
		runMode     contract.RunMode
		wantCluster bool
		wantLocal   bool
	}{
		{name: "Cluster mode", runMode: contract.RunModeCluster, wantCluster: true},
		{name: "Local mode", runMode: contract.RunModeLocal, wantLocal: true},
		{name: "Unknown mode", runMode: contract.RunMode(999)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RayConfig{RunMode: tt.runMode}
			if got := config.IsClusterMode(); got != tt.wantCluster {
				t.Errorf("IsClusterMode() = %v, want %v", got, tt.wantCluster)
			}
			if got := config.IsLocalMode(); got != tt.wantLocal {
				t.Errorf("IsLocalMode() = %v, want %v", got, tt.wantLocal)
			}
		})
	}
}

// newTestCmd builds a cobra command with all flags the worker config parser reads.
func newTestCmd() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().String(GcsAddress, "", "")
	cmd.Flags().String(CodeSearchPath, "", "")
	cmd.Flags().String(RedisUsername, "", "")
	cmd.Flags().String(RedisPassword, "", "")
	cmd.Flags().String(JobID, "", "")
	cmd.Flags().String(ClusterID, "", "")
	cmd.Flags().String(RayletName, "", "")
	cmd.Flags().String(PlasmaStoreName, "", "")
	cmd.Flags().String(SessionDir, "", "")
	cmd.Flags().String(LogsDir, "", "")
	cmd.Flags().String(NodeIpAddress, "", "")
	cmd.Flags().String(HeadArgs, "", "")
	cmd.Flags().String(DefaultActorLifetime, "", "")
	cmd.Flags().String(RuntimeEnvFlag, "", "")
	cmd.Flags().String(JobNamespace, "", "")
	cmd.Flags().Int(NodeManagerPort, 6379, "")
	cmd.Flags().Int(StartupToken, -1, "")
	cmd.Flags().Int(RuntimeEnvHashFlag, -1, "")
	cmd.Flags().String(WorkerIDFlag, "", "")
	return cmd
}

func TestNewRayConfig_NilCmd(t *testing.T) {
	config, err := NewRayConfig(nil, false, true)
	if err == nil {
		t.Error("expected error when cmd is nil, got nil")
	}
	if config != nil {
		t.Errorf("expected nil config when cmd is nil, got %v", config)
	}
}

func TestNewRayConfig_InvalidNodeManagerPort(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(NodeManagerPort, "-1")
	config, err := NewRayConfig(cmd, false, true)
	if err == nil {
		t.Error("expected error for invalid node manager port, got nil")
	}
	if config != nil {
		t.Errorf("expected nil config for invalid port, got %v", config)
	}
}

func TestNewRayConfig_PortTooLarge(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(NodeManagerPort, "70000")
	config, err := NewRayConfig(cmd, false, true)
	if err == nil {
		t.Error("expected error for port too large, got nil")
	}
	if config != nil {
		t.Errorf("expected nil config for port too large, got %v", config)
	}
}

func TestNewRayConfig_ValidWorker(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(StartupToken, "12345")
	cmd.Flags().Set(RuntimeEnvHashFlag, "100")
	cmd.Flags().Set(NodeManagerPort, "6379")

	config, err := NewRayConfig(cmd, false, true)
	if err != nil {
		t.Fatalf("unexpected error for valid worker config: %v", err)
	}
	if config.WorkerType != options.WorkerTypeWorker {
		t.Errorf("WorkerType = %v, want %v", config.WorkerType, options.WorkerTypeWorker)
	}
	if config.RunMode != contract.RunModeCluster {
		t.Errorf("RunMode = %v, want %v", config.RunMode, contract.RunModeCluster)
	}
	if config.NodeManagerPort != 6379 {
		t.Errorf("NodeManagerPort = %d, want %d", config.NodeManagerPort, 6379)
	}
	if config.StartupToken != 12345 {
		t.Errorf("StartupToken = %d, want %d", config.StartupToken, 12345)
	}
}

func TestNewRayConfig_LocalModeDriver(t *testing.T) {
	config, err := NewRayConfig(newTestCmd(), true, false)
	if err != nil {
		t.Fatalf("unexpected error for local mode config: %v", err)
	}
	if config.RunMode != contract.RunModeLocal {
		t.Errorf("RunMode = %v, want %v", config.RunMode, contract.RunModeLocal)
	}
	if config.WorkerType != options.WorkerTypeDriver {
		t.Errorf("WorkerType = %v, want %v", config.WorkerType, options.WorkerTypeDriver)
	}
}

func TestNewRayConfig_WithRayAddress(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(GcsAddress, "192.168.1.100:6379")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.BootstrapIP != "192.168.1.100" {
		t.Errorf("BootstrapIP = %q, want %q", config.BootstrapIP, "192.168.1.100")
	}
	if config.BootstrapPort != 6379 {
		t.Errorf("BootstrapPort = %d, want %d", config.BootstrapPort, 6379)
	}
}

func TestNewRayConfig_WithRayAddressEnv(t *testing.T) {
	oldRayAddress := os.Getenv("RAY_ADDRESS")
	defer os.Setenv("RAY_ADDRESS", oldRayAddress)
	os.Setenv("RAY_ADDRESS", "192.168.1.200:6379")

	config, err := NewRayConfig(newTestCmd(), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.BootstrapIP != "192.168.1.200" {
		t.Errorf("BootstrapIP = %q, want %q", config.BootstrapIP, "192.168.1.200")
	}
	if config.BootstrapPort != 6379 {
		t.Errorf("BootstrapPort = %d, want %d", config.BootstrapPort, 6379)
	}
}

func TestNewRayConfig_WithCodeSearchPath(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(CodeSearchPath, "/path/to/lib1:/path/to/lib2:/path/to/lib3")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedPaths := []string{"/path/to/lib1", "/path/to/lib2", "/path/to/lib3"}
	if len(config.CodeSearchPath) != len(expectedPaths) {
		t.Fatalf("CodeSearchPath length = %d, want %d", len(config.CodeSearchPath), len(expectedPaths))
	}
	for i, want := range expectedPaths {
		if config.CodeSearchPath[i] != want {
			t.Errorf("CodeSearchPath[%d] = %q, want %q", i, config.CodeSearchPath[i], want)
		}
	}
}

func TestNewRayConfig_WithHeadArgs(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(HeadArgs, "--num-cpus=4 --num-gpus=2")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(config.HeadArgs) != 2 {
		t.Errorf("HeadArgs length = %d, want %d", len(config.HeadArgs), 2)
	}
}

func TestNewRayConfig_WithRuntimeEnv(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(RuntimeEnvFlag, `{"pip": ["requests"]}`)
	cmd.Flags().Set(RuntimeEnvHashFlag, "100")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.RuntimeEnv == "" {
		t.Error("expected RuntimeEnv to be set, got empty string")
	}
	if config.RuntimeEnvHash != 100 {
		t.Errorf("RuntimeEnvHash = %d, want %d", config.RuntimeEnvHash, 100)
	}
}

func TestNewRayConfig_WithInvalidRuntimeEnv(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(RuntimeEnvFlag, `{invalid json}`)

	config, err := NewRayConfig(cmd, false, false)
	if err == nil {
		t.Error("expected error for invalid runtime env, got nil")
	}
	if config != nil {
		t.Errorf("expected nil config for invalid runtime env, got %v", config)
	}
}

func TestNewRayConfig_DriverWithNamespace(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(JobNamespace, "test-namespace")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.RayNamespace != "test-namespace" {
		t.Errorf("RayNamespace = %q, want %q", config.RayNamespace, "test-namespace")
	}
}

func TestNewRayConfig_DriverWithoutNamespace(t *testing.T) {
	config, err := NewRayConfig(newTestCmd(), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.RayNamespace == "" {
		t.Error("expected generated RayNamespace, got empty string")
	}
}

func TestNewRayConfig_WorkerWithSessionAndLogsDir(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(SessionDir, "/tmp/ray/session")
	cmd.Flags().Set(LogsDir, "/tmp/ray/logs")

	config, err := NewRayConfig(cmd, false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.SessionDir != "/tmp/ray/session" {
		t.Errorf("SessionDir = %q, want %q", config.SessionDir, "/tmp/ray/session")
	}
	if config.LogsDir != "/tmp/ray/logs" {
		t.Errorf("LogsDir = %q, want %q", config.LogsDir, "/tmp/ray/logs")
	}
}

func TestNewRayConfig_WithActorLifetime(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(DefaultActorLifetime, "detached")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.DefaultActorLifetime != Detached {
		t.Errorf("DefaultActorLifetime = %v, want %v", config.DefaultActorLifetime, Detached)
	}
}

func TestNewRayConfig_WithInvalidActorLifetime(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(DefaultActorLifetime, "invalid")

	config, err := NewRayConfig(cmd, false, false)
	if err == nil {
		t.Error("expected error for invalid actor lifetime, got nil")
	}
	if config != nil {
		t.Errorf("expected nil config for invalid actor lifetime, got %v", config)
	}
}

func TestNewRayConfig_WorkerWithNodeIPAddress(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(NodeIpAddress, "192.168.1.100")

	config, err := NewRayConfig(cmd, false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.NodeIPAddress != "192.168.1.100" {
		t.Errorf("NodeIPAddress = %q, want %q", config.NodeIPAddress, "192.168.1.100")
	}
}

func TestNewRayConfig_WithJobID(t *testing.T) {
	cmd := newTestCmd()
	cmd.Flags().Set(JobID, "test-job-123")

	config, err := NewRayConfig(cmd, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.JobID != "test-job-123" {
		t.Errorf("JobID = %q, want %q", config.JobID, "test-job-123")
	}
}

func TestToAbsolutePath(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "Relative path", path: "relative/path"},
		{name: "Absolute path", path: "/absolute/path"},
		{name: "Current directory", path: "."},
		{name: "Path with double slashes", path: "path//with//slashes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toAbsolutePath(tt.path)
			if err != nil {
				t.Fatalf("toAbsolutePath(%q) error: %v", tt.path, err)
			}
			if result == "" {
				t.Errorf("toAbsolutePath(%q) returned empty string", tt.path)
			}
		})
	}
}

func TestGenerateRandomNamespace(t *testing.T) {
	a := generateRandomNamespace()
	if a == "" {
		t.Fatal("expected non-empty namespace, got empty string")
	}
	b := generateRandomNamespace()
	if b == "" {
		t.Fatal("expected non-empty namespace for the second call, got empty string")
	}
	if !strings.Contains(a, "-") && !strings.HasPrefix(a, "ns-") {
		t.Errorf("unexpected namespace format: %q", a)
	}
}
