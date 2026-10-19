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

// runtime_env_driver is a Ray Go driver that verifies the user-facing RuntimeEnv
// API and the autoscaler state report path end to end on a real cluster. It
// exercises the surfaces where the Go API previously trailed Java and C++:
//
//	User-facing RuntimeEnv API: was missing — users could only hand-write a
//	    runtime_env JSON string. This driver exercises the full value-object
//	    API (NewRuntimeEnv / NewRuntimeEnvFromJSON, Set/SetJSON/Get/GetJSON/
//	    Contains/Remove/IsEmpty, SetConfig/GetConfig, Serialize /
//	    SerializeToRuntimeEnvInfo), then submits runtime envs built through the
//	    API via WithRuntimeEnv over both the actor creation path
//	    (ActorCreationOptions) and the task path (CallOptions), verifying the
//	    workers actually receive them (RuntimeContext.RuntimeEnv()), including
//	    the "config" key that worker-side extractConfigField parses
//	    (internal/runtime_env/utils.go).
//	Autoscaler state reporting: was a placeholder
//	    (go/internal/gcs/native/autoscaler_cgo.go returned ErrNotImplemented).
//	    This driver asserts the GCS client resolves to a production
//	    implementation and exercises the report -> query round trip:
//	    ReportAutoscalingState returns nil and GetAutoscalerStatus still
//	    resolves (end-to-end closed loop).
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	bazel build //go/e2e/runtime_env_driver
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/runtime_env_driver_/runtime_env_driver job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./runtime_env_driver
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// initRay initializes the Ray runtime with a JobConfig that carries the
// userfuncs.so code search path, so the RuntimeEnvProbe actor created below can
// be resolved by the Go worker. The GCS address is read from the RAY_ADDRESS
// environment variable (set by `ray job submit`).
func initRay() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")

	jobConfigBuilder := options.NewJobConfigBuilder().
		WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		return fmt.Errorf("ray runtime init failed: %w", err)
	}
	log.Printf("driver code search path: %s", codeSearchPath)
	return nil
}

// ensureGCSClientRegistered forces the native runtime to create the GCS client
// (lazily initialized on first query) so that ConnectClient registers the
// global singleton (gcs.SetClient). Without this, gcs.GetClient() would report
// ErrNotImplemented. A RuntimeContext cluster query is the cheapest way to
// trigger the lazy GCS client initialization.
func ensureGCSClientRegistered() error {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	// Querying cluster state initializes the GCS client and therefore the
	// gcs.Client singleton. The result itself is validated later.
	_ = ctx.GetAllNodeInfo()
	return nil
}

// unmarshalJSON is a small helper that fails the run on invalid JSON.
func unmarshalJSON(s string) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("invalid JSON %q: %w", s, err)
	}
	return m, nil
}

// testRuntimeEnvAPI verifies the user-facing value-object API in the
// driver process: construction via the internal() thin layer (which needs an
// initialized runtime), field-level Set/SetJSON/Get/GetJSON/Contains/Remove/
// IsEmpty, config Set/Get, Serialize output (including the "config" key), the
// Serialize -> NewRuntimeEnvFromJSON round trip, and SerializeToRuntimeEnvInfo
// (protojson, lowerCamelCase).
func testRuntimeEnvAPI() error {
	// NewRuntimeEnv returns an empty env (runtime is initialized in cluster mode,
	// so internal() resolves to the native runtime).
	env, err := api.NewRuntimeEnv()
	if err != nil {
		return fmt.Errorf("NewRuntimeEnv failed: %w", err)
	}
	if !env.IsEmpty() {
		return fmt.Errorf("new RuntimeEnv should be empty")
	}
	fmt.Println("  NewRuntimeEnv() -> empty env ✓")

	// Set/Get/Contains/IsEmpty on a field.
	if err := env.Set("working_dir", "/tmp/x"); err != nil {
		return fmt.Errorf("Set(working_dir) failed: %w", err)
	}
	if v, ok := env.Get("working_dir"); !ok || v != "/tmp/x" {
		return fmt.Errorf("Get(working_dir) = %v, %v; want /tmp/x, true", v, ok)
	}
	if !env.Contains("working_dir") {
		return fmt.Errorf("Contains(working_dir) should be true")
	}
	if env.Contains("missing") {
		return fmt.Errorf("Contains(missing) should be false")
	}
	if env.IsEmpty() {
		return fmt.Errorf("env should not be empty after Set")
	}
	fmt.Println("  Set/Get/Contains/IsEmpty on fields ✓")

	// SetJSON stores a JSON value for a field (e.g. pip list).
	if err := env.SetJSON("pip", `["numpy"]`); err != nil {
		return fmt.Errorf("SetJSON(pip) failed: %w", err)
	}
	if v, ok := env.Get("pip"); !ok || v == nil {
		return fmt.Errorf("Get(pip) missing after SetJSON")
	}
	if _, err := env.GetJSON("pip"); err != nil {
		return fmt.Errorf("GetJSON(pip) failed: %w", err)
	}
	// GetJSON on a missing field must error (Go idiom).
	if _, err := env.GetJSON("missing"); err == nil {
		return fmt.Errorf("GetJSON(missing) should error")
	}
	fmt.Println("  SetJSON/GetJSON ✓")

	// Remove deletes a field and reports whether it existed.
	if !env.Remove("working_dir") {
		return fmt.Errorf("Remove(working_dir) should return true")
	}
	if env.Remove("working_dir") {
		return fmt.Errorf("Remove(working_dir) twice should return false")
	}
	if env.Contains("working_dir") {
		return fmt.Errorf("Contains(working_dir) should be false after Remove")
	}
	fmt.Println("  Remove ✓")

	// SetConfig writes the config under the "config" key; GetConfig reads it back.
	cfg := &api.RuntimeEnvConfig{
		SetupTimeoutSeconds: 600,
		EagerInstall:        true,
		LogFiles:            []string{"a.log"},
	}
	if err := env.SetConfig(cfg); err != nil {
		return fmt.Errorf("SetConfig failed: %w", err)
	}
	gotCfg := env.GetConfig()
	if gotCfg == nil {
		return fmt.Errorf("GetConfig() returned nil after SetConfig")
	}
	if gotCfg.SetupTimeoutSeconds != 600 || !gotCfg.EagerInstall ||
		len(gotCfg.LogFiles) != 1 || gotCfg.LogFiles[0] != "a.log" {
		return fmt.Errorf("GetConfig() = %+v, want SetupTimeoutSeconds=600 EagerInstall=true LogFiles=[a.log]", gotCfg)
	}
	fmt.Println("  SetConfig/GetConfig round trip ✓")

	// Serialize() outputs the full fields map including the "config" key,
	// directly consumable by WithRuntimeEnv (worker-side extractConfigField
	// reads FieldConfig="config").
	serialized, err := env.Serialize()
	if err != nil {
		return fmt.Errorf("Serialize failed: %w", err)
	}
	m, err := unmarshalJSON(serialized)
	if err != nil {
		return err
	}
	if _, ok := m["pip"]; !ok {
		return fmt.Errorf("Serialize output missing pip: %s", serialized)
	}
	if _, ok := m["config"]; !ok {
		return fmt.Errorf("Serialize output missing config key: %s", serialized)
	}
	cfgObj, ok := m["config"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("Serialize config is not a JSON object: %s", serialized)
	}
	if _, ok := cfgObj["setup_timeout_seconds"]; !ok {
		return fmt.Errorf("Serialize config missing setup_timeout_seconds: %s", serialized)
	}
	fmt.Printf("  Serialize() = %s ✓ (contains fields + config key)\n", serialized)

	// Serialize -> NewRuntimeEnvFromJSON round trip.
	env2, err := api.NewRuntimeEnvFromJSON(serialized)
	if err != nil {
		return fmt.Errorf("NewRuntimeEnvFromJSON failed: %w", err)
	}
	s2, err := env2.Serialize()
	if err != nil {
		return fmt.Errorf("Serialize (round trip) failed: %w", err)
	}
	if s2 != serialized {
		return fmt.Errorf("round-trip mismatch:\n got: %s\nwant: %s", s2, serialized)
	}
	fmt.Println("  NewRuntimeEnvFromJSON round trip ✓")

	// SerializeToRuntimeEnvInfo outputs protojson with lowerCamelCase
	// fields serializedRuntimeEnv + runtimeEnvConfig.
	infoJSON, err := env.SerializeToRuntimeEnvInfo()
	if err != nil {
		return fmt.Errorf("SerializeToRuntimeEnvInfo failed: %w", err)
	}
	info, err := unmarshalJSON(infoJSON)
	if err != nil {
		return err
	}
	if _, ok := info["serializedRuntimeEnv"]; !ok {
		return fmt.Errorf("SerializeToRuntimeEnvInfo missing serializedRuntimeEnv: %s", infoJSON)
	}
	if _, ok := info["runtimeEnvConfig"]; !ok {
		return fmt.Errorf("SerializeToRuntimeEnvInfo missing runtimeEnvConfig: %s", infoJSON)
	}
	if !strings.Contains(infoJSON, "setupTimeoutSeconds") {
		return fmt.Errorf("SerializeToRuntimeEnvInfo config missing setupTimeoutSeconds (lowerCamelCase): %s", infoJSON)
	}
	fmt.Printf("  SerializeToRuntimeEnvInfo() = %s ✓\n", infoJSON)

	// Set("config", non-config) must be rejected (type check).
	if err := env.Set("config", "not-a-config"); err == nil {
		return fmt.Errorf("Set(config, string) should error")
	}
	fmt.Println("  Set(config, wrong type) rejected ✓")

	// Set("config", map) is the JSON-compatible map branch of Set.
	envMap, err := api.NewRuntimeEnv()
	if err != nil {
		return fmt.Errorf("NewRuntimeEnv (map branch) failed: %w", err)
	}
	if err := envMap.Set("config", map[string]interface{}{"setup_timeout_seconds": 120}); err != nil {
		return fmt.Errorf("Set(config, map) failed: %w", err)
	}
	if cfg := envMap.GetConfig(); cfg == nil || cfg.SetupTimeoutSeconds != 120 {
		return fmt.Errorf("GetConfig after Set(config, map) = %+v, want SetupTimeoutSeconds=120", cfg)
	}
	fmt.Println("  Set(config, map) accepted + GetConfig reads it back ✓")

	// SetConfig validation: setup_timeout_seconds must be > 0 or -1.
	badCfg := &api.RuntimeEnvConfig{SetupTimeoutSeconds: 0}
	if err := envMap.SetConfig(badCfg); err == nil {
		return fmt.Errorf("SetConfig(SetupTimeoutSeconds=0) should error")
	}
	fmt.Println("  SetConfig(SetupTimeoutSeconds=0) rejected ✓")

	// GetConfig on an env without a config returns nil (not a panic).
	noCfg, err := api.NewRuntimeEnv()
	if err != nil {
		return fmt.Errorf("NewRuntimeEnv (no config) failed: %w", err)
	}
	if got := noCfg.GetConfig(); got != nil {
		return fmt.Errorf("GetConfig() on empty env = %+v, want nil", got)
	}
	fmt.Println("  GetConfig() on empty env returns nil ✓")

	// SerializeToRuntimeEnvInfo without a config emits only serializedRuntimeEnv
	// (no runtimeEnvConfig field).
	infoNoCfg, err := noCfg.SerializeToRuntimeEnvInfo()
	if err != nil {
		return fmt.Errorf("SerializeToRuntimeEnvInfo (no config) failed: %w", err)
	}
	infoNoCfgMap, err := unmarshalJSON(infoNoCfg)
	if err != nil {
		return err
	}
	if _, ok := infoNoCfgMap["serializedRuntimeEnv"]; !ok {
		return fmt.Errorf("SerializeToRuntimeEnvInfo (no config) missing serializedRuntimeEnv: %s", infoNoCfg)
	}
	if _, ok := infoNoCfgMap["runtimeEnvConfig"]; ok {
		return fmt.Errorf("SerializeToRuntimeEnvInfo (no config) unexpectedly has runtimeEnvConfig: %s", infoNoCfg)
	}
	fmt.Printf("  SerializeToRuntimeEnvInfo() without config emits no runtimeEnvConfig ✓: %s\n", infoNoCfg)

	// SetJSON with invalid JSON must error (not panic).
	if err := noCfg.SetJSON("pip", "{invalid"); err == nil {
		return fmt.Errorf("SetJSON(invalid JSON) should error")
	}
	fmt.Println("  SetJSON(invalid JSON) rejected ✓")

	fmt.Println("  RuntimeEnv value-object API verified ✓")
	return nil
}

// testRuntimeEnvSubmit builds a runtime env through the API, submits it to an
// actor via WithRuntimeEnv, and verifies the worker actually receives it: the
// actor method reads RuntimeContext.RuntimeEnv() and the driver asserts the
// returned JSON contains the submitted fields and the "config" key (so
// worker-side extractConfigField can parse the config). This exercises the full
// submission path: api.RuntimeEnv -> SerializeToRuntimeEnvInfo (the
// RuntimeEnvInfo JSON that Java CallOptions/ActorCreationOptions also submit,
// CallOptions.java:24) -> WithRuntimeEnv -> native submitter
// (native_submitter.go:341/382) -> C++ OverrideTaskOrActorRuntimeEnvInfo
// (core_worker.cc:1800, JsonStringToMessage parses it into rpc::RuntimeEnvInfo)
// -> worker RuntimeContext.RuntimeEnv() (serialized_runtime_env field).
//
// Note: the submission string must be the full RuntimeEnvInfo JSON
// (SerializeToRuntimeEnvInfo), not the bare field object (Serialize): C++
// JsonStringToMessage rejects unknown proto fields such as a top-level
// "working_dir".
func testRuntimeEnvSubmit() error {
	env, err := api.NewRuntimeEnv()
	if err != nil {
		return fmt.Errorf("NewRuntimeEnv failed: %w", err)
	}
	if err := env.Set("env_vars", map[string]interface{}{"GO_E2E_RUNTIME_ENV": "present"}); err != nil {
		return fmt.Errorf("Set(env_vars) failed: %w", err)
	}
	if err := env.SetConfig(&api.RuntimeEnvConfig{
		SetupTimeoutSeconds: 600,
		EagerInstall:        false,
		LogFiles:            []string{"e2e.log"},
	}); err != nil {
		return fmt.Errorf("SetConfig failed: %w", err)
	}
	// Full RuntimeEnvInfo JSON: this is what the submission path consumes
	// (aligned with Java CallOptions.serializeToRuntimeEnvInfo()).
	infoJSON, err := env.SerializeToRuntimeEnvInfo()
	if err != nil {
		return fmt.Errorf("SerializeToRuntimeEnvInfo failed: %w", err)
	}

	// Create the probe actor with the RuntimeEnvInfo JSON attached.
	handle, err := api.Actor[*userfuncs.RuntimeEnvProbe]((*userfuncs.RuntimeEnvProbe)(nil)).
		WithRuntimeEnv(infoJSON).
		Create()
	if err != nil {
		return fmt.Errorf("create RuntimeEnvProbe actor with runtime env failed: %w", err)
	}
	if handle == nil || handle.ID().IsNil() {
		return fmt.Errorf("expected a non-nil actor handle")
	}
	fmt.Printf("  created RuntimeEnvProbe actor (ID=%s) with runtime env\n", handle.ID().Hex())

	typed := api.NewActorHandleImpl[string](handle.ID())
	ref, err := typed.Task((*userfuncs.RuntimeEnvProbe).RuntimeEnv).Remote()
	if err != nil {
		return fmt.Errorf("RuntimeEnvProbe.RuntimeEnv() submission failed: %w", err)
	}
	workerEnv, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get RuntimeEnvProbe.RuntimeEnv() result failed: %w", err)
	}
	if workerEnv == "" {
		return fmt.Errorf("worker reported an empty runtime env; WithRuntimeEnv not received")
	}
	worker, err := unmarshalJSON(workerEnv)
	if err != nil {
		return fmt.Errorf("worker runtime env is not valid JSON: %w", err)
	}
	// Worker side returns the serialized_runtime_env field (bare JSON, merged
	// with the parent job-level runtime env by C++ OverrideRuntimeEnv), so the
	// submitted fields must be present; the "config" key must survive for
	// worker-side extractConfigField (internal/runtime_env/utils.go:145). The
	// job-level runtime env (ray job submit --working-dir .) carries a gcs://
	// working_dir URI that the actor-level env does not override, so the worker
	// must inherit it. Assert the value is a non-empty string: a bare key check
	// would pass on a null/empty value and report a working_dir that is not
	// actually usable.
	wd, ok := worker["working_dir"]
	if !ok {
		return fmt.Errorf("worker runtime env missing working_dir (inherited from job env): %s", workerEnv)
	}
	wdStr, isStr := wd.(string)
	if !isStr || wdStr == "" {
		return fmt.Errorf("worker runtime env working_dir is not a non-empty string (got %v): %s", wd, workerEnv)
	}
	if v, ok := worker["env_vars"].(map[string]interface{}); !ok || v["GO_E2E_RUNTIME_ENV"] != "present" {
		return fmt.Errorf("worker runtime env missing GO_E2E_RUNTIME_ENV env_var: %s", workerEnv)
	}
	if _, ok := worker["config"]; !ok {
		return fmt.Errorf("worker runtime env missing config key (extractConfigField input): %s", workerEnv)
	}
	fmt.Printf("  worker received runtime env with working_dir + env_vars + config ✓: %s\n", workerEnv)
	fmt.Println("  worker serialized_runtime_env carries submitted fields + config key ✓")

	// Release the actor so its worker can be recycled.
	if _, err := typed.Task((*userfuncs.RuntimeEnvProbe).Exit).Remote(); err != nil {
		return fmt.Errorf("submit RuntimeEnvProbe.Exit for cleanup failed: %w", err)
	}
	fmt.Println("  RuntimeEnv submission verified ✓")
	return nil
}

// testRuntimeEnvTaskSubmit verifies the task-level runtime env submission path:
// CallOptions.WithRuntimeEnv routes through the SubmitTask path
// (go/internal/runtime/cgo/native_submitter.go:341), in contrast to the actor
// creation path exercised by testRuntimeEnvSubmit (native_submitter.go:382).
// A remote task worker reads RuntimeContext.RuntimeEnv() and the driver asserts
// the submitted fields and the "config" key are observed there.
func testRuntimeEnvTaskSubmit() error {
	env, err := api.NewRuntimeEnv()
	if err != nil {
		return fmt.Errorf("NewRuntimeEnv failed: %w", err)
	}
	if err := env.Set("env_vars", map[string]interface{}{"GO_E2E_TASK_RUNTIME_ENV": "present"}); err != nil {
		return fmt.Errorf("Set(env_vars) failed: %w", err)
	}
	if err := env.SetConfig(&api.RuntimeEnvConfig{
		SetupTimeoutSeconds: 300,
		EagerInstall:        false,
		LogFiles:            []string{"task.log"},
	}); err != nil {
		return fmt.Errorf("SetConfig failed: %w", err)
	}
	infoJSON, err := env.SerializeToRuntimeEnvInfo()
	if err != nil {
		return fmt.Errorf("SerializeToRuntimeEnvInfo failed: %w", err)
	}

	// Submit a remote task carrying the runtime env via CallOptions.
	ref, err := api.Remote[string](userfuncs.GetRuntimeEnvOfTask()).
		WithRuntimeEnv(infoJSON).
		Call()
	if err != nil {
		return fmt.Errorf("task submission with runtime env failed: %w", err)
	}
	workerEnv, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get runtimeEnvOfTask result failed: %w", err)
	}
	if workerEnv == "" {
		return fmt.Errorf("task worker reported an empty runtime env; CallOptions.WithRuntimeEnv not received")
	}
	worker, err := unmarshalJSON(workerEnv)
	if err != nil {
		return fmt.Errorf("task worker runtime env is not valid JSON: %w", err)
	}
	if v, ok := worker["env_vars"].(map[string]interface{}); !ok || v["GO_E2E_TASK_RUNTIME_ENV"] != "present" {
		return fmt.Errorf("task worker runtime env missing GO_E2E_TASK_RUNTIME_ENV env_var: %s", workerEnv)
	}
	if _, ok := worker["config"]; !ok {
		return fmt.Errorf("task worker runtime env missing config key: %s", workerEnv)
	}
	fmt.Printf("  task worker received runtime env with env_vars + config ✓: %s\n", workerEnv)
	fmt.Println("  Task-level runtime env submission verified ✓ (CallOptions path)")
	return nil
}

// testAutoscalerReport verifies the autoscaler state reporting path
// is no longer a placeholder. The GCS client must resolve to a production
// implementation and ReportAutoscalingState must return nil on a live cluster;
// afterwards GetAutoscalerStatus still resolves, closing the loop.
func testAutoscalerReport() error {
	if err := ensureGCSClientRegistered(); err != nil {
		return err
	}

	client, err := gcs.GetClient()
	if err != nil {
		return fmt.Errorf("gcs.GetClient() returned %v, want a production client", err)
	}
	if client == nil {
		return fmt.Errorf("gcs.GetClient() returned nil client")
	}
	fmt.Println("  gcs.GetClient() resolves to a production client ✓")

	// Serialize an AutoscalingState and report it. This is exactly what
	// AutoscalerMonitor.reportAutoscalingState does (monitor.go:318-327).
	state := &proto.AutoscalingState{
		AutoscalerStateVersion:              1,
		LastSeenClusterResourceStateVersion: 1,
	}
	stateBytes, err := protolib.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal AutoscalingState failed: %w", err)
	}
	if err := client.ReportAutoscalingState(string(stateBytes)); err != nil {
		return fmt.Errorf("ReportAutoscalingState failed: %w", err)
	}
	fmt.Println("  ReportAutoscalingState(serialized AutoscalingState) -> nil ✓")

	// Closed loop: the query path still resolves after the report.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := client.GetAutoscalerStatus(ctx)
	if err != nil {
		return fmt.Errorf("GetAutoscalerStatus after report failed: %w", err)
	}
	if status == nil {
		return fmt.Errorf("GetAutoscalerStatus returned nil reply")
	}
	fmt.Printf("  GetAutoscalerStatus after report resolves ✓ (autoscaling_state present: %v)\n",
		status.AutoscalingState != nil)

	fmt.Println("  Autoscaler state report verified ✓")
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestRuntimeEnvAPI ===")
	if err := testRuntimeEnvAPI(); err != nil {
		log.Fatalf("TestRuntimeEnvAPI FAILED: %v", err)
	}
	fmt.Println("=== TestRuntimeEnvAPI PASSED ===\n")

	fmt.Println("=== TestRuntimeEnvSubmit ===")
	if err := testRuntimeEnvSubmit(); err != nil {
		log.Fatalf("TestRuntimeEnvSubmit FAILED: %v", err)
	}
	fmt.Println("=== TestRuntimeEnvSubmit PASSED ===\n")

	fmt.Println("=== TestRuntimeEnvTaskSubmit ===")
	if err := testRuntimeEnvTaskSubmit(); err != nil {
		log.Fatalf("TestRuntimeEnvTaskSubmit FAILED: %v", err)
	}
	fmt.Println("=== TestRuntimeEnvTaskSubmit PASSED ===\n")

	fmt.Println("=== TestAutoscalerReport ===")
	if err := testAutoscalerReport(); err != nil {
		log.Fatalf("TestAutoscalerReport FAILED: %v", err)
	}
	fmt.Println("=== TestAutoscalerReport PASSED ===\n")

	fmt.Println("All RuntimeEnv / autoscaler-report checks passed.")
}
