//go:build apiruntimeenv

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

package api

import (
	"encoding/json"
	"errors"
	"testing"

	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// runtimeEnvTestRuntime is a contract.Runtime whose CreateRuntimeEnv returns an
// empty map and DeserializeRuntimeEnv performs json.Unmarshal. It embeds
// recordingRuntime (defined in task_return_ref_test.go) to inherit all other
// contract.Runtime methods, preserving dependency inversion (no go/internal).
type runtimeEnvTestRuntime struct {
	recordingRuntime
}

// newRuntimeEnvTest installs the mock runtime handle and returns it.
func newRuntimeEnvTest() runtimeEnvTestRuntime {
	rt := runtimeEnvTestRuntime{}
	storeHandleLocked(recordingHandle{rt: rt})
	return rt
}

// nilRuntimeEnvTestRuntime is a contract.Runtime whose CreateRuntimeEnv
// returns a nil map, to verify NewRuntimeEnv normalizes it into a usable empty
// map instead of leaving the returned env in a nil-fields (panic-on-write)
// state.
type nilRuntimeEnvTestRuntime struct {
	recordingRuntime
}

func (nilRuntimeEnvTestRuntime) CreateRuntimeEnv() contract.RuntimeEnv { return nil }

// TestNewRuntimeEnvNilNormalization verifies that NewRuntimeEnv never returns
// an env with nil fields, even when the underlying runtime hands back a nil
// map: the first Set/SetConfig write must not panic.
func TestNewRuntimeEnvNilNormalization(t *testing.T) {
	storeHandleLocked(recordingHandle{rt: nilRuntimeEnvTestRuntime{}})
	defer clearHandle()

	env, err := NewRuntimeEnv()
	if err != nil {
		t.Fatalf("NewRuntimeEnv error: %v", err)
	}
	if !env.IsEmpty() {
		t.Fatalf("normalized env should be empty")
	}
	// A nil-fields state would panic here on the first write.
	if err := env.Set("working_dir", "/tmp/x"); err != nil {
		t.Fatalf("Set on normalized env error: %v", err)
	}
	if err := env.SetConfig(&RuntimeEnvConfig{SetupTimeoutSeconds: 600}); err != nil {
		t.Fatalf("SetConfig on normalized env error: %v", err)
	}
}

// TestNewRuntimeEnvUninitialized verifies AC2: with no handle installed,
// internal() returns rayerrors.ErrRuntimeNotInitialized (internal.go:37-39) and
// NewRuntimeEnv does not panic. Mirrors internal_test.go's
// TestInternalNotInitialized pattern.
func TestNewRuntimeEnvUninitialized(t *testing.T) {
	if IsInitialized() {
		t.Skip("runtime already initialized; run in isolation with -tags apiruntimeenv")
	}
	_, err := NewRuntimeEnv()
	if err == nil || !errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		t.Fatalf("NewRuntimeEnv with no runtime = %v, want ErrRuntimeNotInitialized", err)
	}
}

// TestNewRuntimeEnvFromJSONUninitialized verifies AC2 for the deserialize path.
func TestNewRuntimeEnvFromJSONUninitialized(t *testing.T) {
	if IsInitialized() {
		t.Skip("runtime already initialized; run in isolation with -tags apiruntimeenv")
	}
	_, err := NewRuntimeEnvFromJSON("{}")
	if err == nil || !errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		t.Fatalf("NewRuntimeEnvFromJSON with no runtime = %v, want ErrRuntimeNotInitialized", err)
	}
}

func TestRuntimeEnvSetGetContainsRemoveIsEmpty(t *testing.T) {
	newRuntimeEnvTest()
	env, err := NewRuntimeEnv()
	if err != nil {
		t.Fatalf("NewRuntimeEnv error: %v", err)
	}
	if !env.IsEmpty() {
		t.Fatalf("new env should be empty")
	}
	if err := env.Set("working_dir", "/tmp/x"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if env.IsEmpty() {
		t.Fatalf("env should not be empty after Set")
	}
	if v, ok := env.Get("working_dir"); !ok || v != "/tmp/x" {
		t.Fatalf("Get(working_dir) = %v, %v; want /tmp/x, true", v, ok)
	}
	if !env.Contains("working_dir") {
		t.Fatalf("Contains(working_dir) should be true")
	}
	if env.Contains("missing") {
		t.Fatalf("Contains(missing) should be false")
	}
	if !env.Remove("working_dir") {
		t.Fatalf("Remove(working_dir) should return true")
	}
	if env.Remove("working_dir") {
		t.Fatalf("Remove(working_dir) twice should return false")
	}
}

func TestRuntimeEnvSetJSON(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	if err := env.SetJSON("pip", `["numpy"]`); err != nil {
		t.Fatalf("SetJSON error: %v", err)
	}
	if err := env.SetJSON("bad", `{invalid`); err == nil {
		t.Fatalf("SetJSON with invalid JSON should error")
	}
}

func TestRuntimeEnvSerialize(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	env.Set("working_dir", "/tmp/x")
	env.Set("pip", []interface{}{"numpy"})
	cfg := &RuntimeEnvConfig{SetupTimeoutSeconds: 600, EagerInstall: true, LogFiles: []string{"a.log"}}
	if err := env.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig error: %v", err)
	}
	serialized, err := env.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(serialized), &m); err != nil {
		t.Fatalf("Serialize output not valid JSON: %v", err)
	}
	if _, ok := m["working_dir"]; !ok {
		t.Fatalf("Serialize output missing working_dir: %s", serialized)
	}
	if _, ok := m["config"]; !ok {
		t.Fatalf("Serialize output missing config key: %s", serialized)
	}
	if _, ok := m["config"].(map[string]interface{})["setup_timeout_seconds"]; !ok {
		t.Fatalf("Serialize config missing setup_timeout_seconds: %s", serialized)
	}
}

func TestRuntimeEnvRoundTrip(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	env.Set("working_dir", "/tmp/x")
	env.Set("pip", []interface{}{"numpy"})
	serialized, err := env.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}
	env2, err := NewRuntimeEnvFromJSON(serialized)
	if err != nil {
		t.Fatalf("NewRuntimeEnvFromJSON error: %v", err)
	}
	s2, _ := env2.Serialize()
	if s2 != serialized {
		t.Fatalf("round-trip mismatch:\n got: %s\nwant: %s", s2, serialized)
	}
}

func TestRuntimeEnvSerializeToRuntimeEnvInfo(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	env.Set("working_dir", "/tmp/x")
	env.SetConfig(&RuntimeEnvConfig{SetupTimeoutSeconds: 600, EagerInstall: true, LogFiles: []string{"a.log"}})
	out, err := env.SerializeToRuntimeEnvInfo()
	if err != nil {
		t.Fatalf("SerializeToRuntimeEnvInfo error: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("SerializeToRuntimeEnvInfo output not valid JSON: %v", err)
	}
	if _, ok := m["serializedRuntimeEnv"]; !ok {
		t.Fatalf("missing serializedRuntimeEnv field: %s", out)
	}
	if _, ok := m["runtimeEnvConfig"]; !ok {
		t.Fatalf("missing runtimeEnvConfig field: %s", out)
	}
}

func TestRuntimeEnvSetConfigTypeCheck(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	if err := env.Set("config", "not-a-config"); err == nil {
		t.Fatalf("Set(config, string) should error")
	}
	if err := env.Set("config", map[string]interface{}{"setup_timeout_seconds": float64(600)}); err != nil {
		t.Fatalf("Set(config, map) should be allowed: %v", err)
	}
}

// TestRuntimeEnvSetConfigMapValidation verifies that the JSON-compatible map
// branch of Set("config", ...) normalizes a missing setup_timeout_seconds to
// the default 600 (aligned with worker-side ParseAndValidateRuntimeEnvConfig /
// FromProtoRuntimeEnvConfig), rejects explicit invalid values, and clears on a
// nil map.
func TestRuntimeEnvSetConfigMapValidation(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	// Missing setup_timeout_seconds is normalized to the default 600, so an
	// empty map and an explicit 0 are both accepted.
	if err := env.Set("config", map[string]interface{}{}); err != nil {
		t.Fatalf("Set(config, empty map) should be accepted (defaults to 600): %v", err)
	}
	if c := env.GetConfig(); c == nil || c.SetupTimeoutSeconds != 600 {
		t.Fatalf("GetConfig after Set(config, empty map) = %+v, want SetupTimeoutSeconds=600", c)
	}
	if err := env.Set("config", map[string]interface{}{"setup_timeout_seconds": float64(0)}); err != nil {
		t.Fatalf("Set(config, map with setup_timeout_seconds=0) should normalize to 600: %v", err)
	}
	// An explicit invalid value (< 0 and != -1) is still rejected.
	if err := env.Set("config", map[string]interface{}{"setup_timeout_seconds": float64(-2)}); err == nil {
		t.Fatalf("Set(config, map with setup_timeout_seconds=-2) should error")
	}
	if err := env.Set("config", map[string]interface{}(nil)); err != nil {
		t.Fatalf("Set(config, nil map) should clear the config: %v", err)
	}
	if c := env.GetConfig(); c != nil {
		t.Fatalf("GetConfig after Set(config, nil map) = %+v, want nil", c)
	}
}

func TestRuntimeEnvGetJSONMissing(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	if _, err := env.GetJSON("missing"); err == nil {
		t.Fatalf("GetJSON(missing) should error")
	}
}

func TestRuntimeEnvGetConfigUnset(t *testing.T) {
	newRuntimeEnvTest()
	env, _ := NewRuntimeEnv()
	if c := env.GetConfig(); c != nil {
		t.Fatalf("GetConfig() on empty env = %v, want nil", c)
	}
}

// TestRuntimeEnvZeroValueUsable verifies that the zero value of the exported
// RuntimeEnv struct (var env RuntimeEnv, bypassing NewRuntimeEnv) is usable:
// every mutator lazily initializes the fields map instead of panicking on a
// nil map assignment.
func TestRuntimeEnvZeroValueUsable(t *testing.T) {
	var env RuntimeEnv
	if err := env.Set("working_dir", "gs://bucket/dir"); err != nil {
		t.Fatalf("Set on zero-value RuntimeEnv: %v", err)
	}
	if err := env.SetConfig(&RuntimeEnvConfig{SetupTimeoutSeconds: 600}); err != nil {
		t.Fatalf("SetConfig on zero-value RuntimeEnv: %v", err)
	}
	if _, ok := env.Get("working_dir"); !ok {
		t.Fatal("Get(working_dir) missing after Set on zero-value RuntimeEnv")
	}
	if c := env.GetConfig(); c == nil || c.SetupTimeoutSeconds != 600 {
		t.Fatalf("GetConfig() after SetConfig on zero value = %+v, want timeout 600", c)
	}
}
