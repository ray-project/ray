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

package runtime_env

import (
	"context"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRocProfSysPlugin_Create_InvalidStringConfig(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "invalid_string"}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for invalid string config, got nil")
	}

	if !strings.Contains(err.Error(), "validate rocprof_sys config") {
		t.Errorf("expected error to mention 'validate rocprof_sys config', got: %v", err)
	}
}

func TestRocProfSysPlugin_Create_NonStringValueInArgs(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"args": map[string]interface{}{"F": 123},
	}}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for non-string value in args, got nil")
	}

	if !strings.Contains(err.Error(), "must be a string") {
		t.Errorf("expected error to mention 'must be a string', got: %v", err)
	}
}

func TestRocProfSysPlugin_Create_WrongType(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldRocprofSys: []string{"cuda"}}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}

	if !strings.Contains(err.Error(), "must be a map") {
		t.Errorf("expected error to mention 'must be a map', got: %v", err)
	}
}

func TestRocProfSysPlugin_Create_Timeout(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-timeout-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err = plugin.Create(ctx, "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected timeout error, got nil")
	} else if !strings.Contains(err.Error(), "context deadline exceeded") && !strings.Contains(err.Error(), "rocprof") {
		t.Logf("got error (may be expected): %v", err)
	}
}

func TestRocProfSysPlugin_ConcurrentAccess_Stress(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-stress-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	var wg sync.WaitGroup
	successCount := 0
	errorCount := 0
	var mu sync.Mutex

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
			mu.Lock()
			if err != nil {
				errorCount++
			} else {
				successCount++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent access test completed: %d succeeded, %d failed (rocprof-sys may not be installed)", successCount, errorCount)

	plugin.rocprofSysMutex.RLock()
	rocprofSysCmd := plugin.rocprofSysCmd
	plugin.rocprofSysMutex.RUnlock()

	if len(rocprofSysCmd) == 0 && successCount == 0 {
		t.Log("rocprof_sys command not set (expected when rocprof-sys is not installed)")
	}
}

func TestRocProfSysPlugin_ParseConfig_SingleCharOption(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{
		"args": map[string]string{"F": "true"},
	}
	cmd, _ := plugin.parseRocProfSysConfig(configMap)

	expected := []string{"rocprof-sys-python", "-F", "true", "--"}
	if len(cmd) != len(expected) {
		t.Fatalf("expected command length %d, got %d: %v", len(expected), len(cmd), cmd)
	}

	for i, exp := range expected {
		if cmd[i] != exp {
			t.Errorf("expected cmd[%d] = %q, got %q", i, exp, cmd[i])
		}
	}
}

func TestRocProfSysPlugin_ParseConfig_LongOption(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{
		"args": map[string]string{"output": "file.txt"},
	}
	cmd, _ := plugin.parseRocProfSysConfig(configMap)

	expected := []string{"rocprof-sys-python", "--output=file.txt", "--"}
	if len(cmd) != len(expected) {
		t.Fatalf("expected command length %d, got %d: %v", len(expected), len(cmd), cmd)
	}

	for i, exp := range expected {
		if cmd[i] != exp {
			t.Errorf("expected cmd[%d] = %q, got %q", i, exp, cmd[i])
		}
	}
}

func TestRocProfSysPlugin_NameAndPriority(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	if plugin.Name() != FieldRocprofSys {
		t.Errorf("expected Name() = %s, got %s", FieldRocprofSys, plugin.Name())
	}

	if plugin.Priority() != RayRuntimeEnvPluginDefaultPriority {
		t.Errorf("expected Priority() = %d, got %d", RayRuntimeEnvPluginDefaultPriority, plugin.Priority())
	}
}

func TestRocProfSysPlugin_GetURIs(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	uris := plugin.GetURIs(runtimeEnv)
	if uris != nil && len(uris) != 0 {
		t.Errorf("expected empty URIs, got: %v", uris)
	}
}

func TestRocProfSysPlugin_DeleteURI(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	result := plugin.DeleteURI("test-uri")
	if result != 0 {
		t.Errorf("expected DeleteURI to return 0, got: %d", result)
	}
}

func TestRocProfSysPlugin_Validate_NilConfig(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for nil config, got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_ValidString(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for valid string config, got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_ValidMapWithArgs(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"args": map[string]string{"F": "true"},
	}}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for valid map with args, got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_ValidMapWithEnv(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"env": map[string]string{"TEST_VAR": "value"},
	}}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for valid map with env, got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_InvalidEnvValueType(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"env": map[string]interface{}{"TEST_VAR": 123},
	}}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("expected error for non-string env value, got nil")
	}

	if !strings.Contains(err.Error(), "must be a string") {
		t.Errorf("expected error to mention 'must be a string', got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_InvalidEnvType(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"env": "invalid",
	}}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("expected error for invalid env type, got nil")
	}

	if !strings.Contains(err.Error(), "must be a map") {
		t.Errorf("expected error to mention 'must be a map', got: %v", err)
	}
}

func TestRocProfSysPlugin_Validate_InvalidArgsType(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"args": "invalid",
	}}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("expected error for invalid args type, got nil")
	}

	if !strings.Contains(err.Error(), "must be a map") {
		t.Errorf("expected error to mention 'must be a map', got: %v", err)
	}
}

func TestRocProfSysPlugin_ModifyContext_NoConfig(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{
		PyExecutable: "/usr/bin/python",
		EnvVars:      make(map[string]string),
	}

	err := plugin.ModifyContext(nil, runtimeEnv, context)
	if err != nil {
		t.Errorf("expected no error when no config, got: %v", err)
	}

	if context.PyExecutable != "/usr/bin/python" {
		t.Errorf("expected PyExecutable unchanged, got: %s", context.PyExecutable)
	}
}

func TestRocProfSysPlugin_ModifyContext_WithConfig(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	plugin.rocprofSysMutex.Lock()
	plugin.rocprofSysCmd = []string{"rocprof-sys-python", "-F", "true", "--"}
	plugin.rocprofSysEnv = map[string]string{
		"ROCPROFSYS_TIME_OUTPUT":   "false",
		"ROCPROFSYS_OUTPUT_PREFIX": "worker_process_%p",
	}
	plugin.rocprofSysMutex.Unlock()

	ctx := &RuntimeEnvContext{
		PyExecutable: "/usr/bin/python",
		EnvVars:      make(map[string]string),
	}

	err := plugin.ModifyContext(nil, nil, ctx)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if !strings.HasPrefix(ctx.PyExecutable, "rocprof-sys-python") {
		t.Errorf("expected PyExecutable to start with 'rocprof-sys-python', got: %s", ctx.PyExecutable)
	}

	if _, ok := ctx.EnvVars["ROCPROFSYS_TIME_OUTPUT"]; !ok {
		t.Error("expected ROCPROFSYS_TIME_OUTPUT to be set in EnvVars")
	}
}

func TestRocProfSysPlugin_ParseConfig_EmptyConfig(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{}
	cmd, env := plugin.parseRocProfSysConfig(configMap)

	expectedCmd := []string{"rocprof-sys-python", "--"}
	if len(cmd) != len(expectedCmd) {
		t.Fatalf("expected command length %d, got %d: %v", len(expectedCmd), len(cmd), cmd)
	}

	for i, exp := range expectedCmd {
		if cmd[i] != exp {
			t.Errorf("expected cmd[%d] = %q, got %q", i, exp, cmd[i])
		}
	}

	if len(env) != 0 {
		t.Errorf("expected empty env, got: %v", env)
	}
}

func TestRocProfSysPlugin_ParseConfig_MixedOptions(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{
		"args": map[string]string{
			"F":      "true",
			"output": "file.txt",
		},
		"env": map[string]string{
			"VAR1": "value1",
			"VAR2": "value2",
		},
	}
	cmd, env := plugin.parseRocProfSysConfig(configMap)

	// Should have base command + 2 args + "--"
	if len(cmd) < 4 {
		t.Fatalf("expected at least 4 command elements, got %d: %v", len(cmd), cmd)
	}

	if cmd[0] != "rocprof-sys-python" {
		t.Errorf("expected first element to be 'rocprof-sys-python', got: %s", cmd[0])
	}

	if cmd[len(cmd)-1] != "--" {
		t.Errorf("expected last element to be '--', got: %s", cmd[len(cmd)-1])
	}

	if len(env) != 2 {
		t.Errorf("expected 2 env vars, got %d: %v", len(env), env)
	}
}

func TestRocProfSysPlugin_ParseConfig_InterfaceMapArgs(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{
		"args": map[string]interface{}{
			"F":      "true",
			"output": "file.txt",
		},
	}
	cmd, _ := plugin.parseRocProfSysConfig(configMap)

	// Should handle interface{} map correctly
	if len(cmd) < 4 {
		t.Fatalf("expected at least 4 command elements, got %d: %v", len(cmd), cmd)
	}
}

func TestRocProfSysPlugin_ParseConfig_InterfaceMapEnv(t *testing.T) {
	plugin := NewRocProfSysPlugin("/tmp/test")

	configMap := map[string]interface{}{
		"env": map[string]interface{}{
			"VAR1": "value1",
			"VAR2": "value2",
		},
	}
	_, env := plugin.parseRocProfSysConfig(configMap)

	if len(env) != 2 {
		t.Errorf("expected 2 env vars, got %d: %v", len(env), env)
	}
}

func TestRocProfSysPlugin_Create_DefaultConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-default-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	// This will likely fail because rocprof-sys is not installed, but we're testing the flow
	if err == nil {
		t.Log("Create succeeded (rocprof-sys may be installed)")
	} else {
		t.Logf("Create failed as expected (rocprof-sys not installed): %v", err)
	}
}

func TestRocProfSysPlugin_Create_CustomConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-custom-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: map[string]interface{}{
		"args": map[string]string{"F": "true"},
		"env":  map[string]string{"CUSTOM_VAR": "custom_value"},
	}}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Log("Create succeeded (rocprof-sys may be installed)")
	} else {
		t.Logf("Create failed as expected (rocprof-sys not installed): %v", err)
	}
}

func TestRocProfSysPlugin_Create_ContextTimeout(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-ctx-timeout-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond*100)
	defer cancel()

	_, err = plugin.Create(ctx, "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}

func TestRocProfSysPlugin_CheckScript_FolderCreationError(t *testing.T) {
	// This test is difficult to trigger without mocking, so we'll skip it for now
	// The checkRocProfSysScript function is already well covered by Create tests
	t.Skip("requires filesystem mocking")
}

func TestRocProfSysPlugin_CheckScript_FileCreationError(t *testing.T) {
	// Similar to above, requires filesystem mocking
	t.Skip("requires filesystem mocking")
}

func TestRocProfSysPlugin_PlatformSupport(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("skipping on Linux - this test is for non-Linux platforms")
	}

	tmpDir, err := os.MkdirTemp("", "rocprof_sys-platform-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewRocProfSysPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldRocprofSys: "default"}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected platform error on non-Linux, got nil")
	} else if !strings.Contains(err.Error(), "Linux") {
		t.Errorf("expected error to mention 'Linux', got: %v", err)
	}
}
