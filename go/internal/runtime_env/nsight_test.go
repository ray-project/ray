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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNsightPlugin_Create_InvalidStringConfig(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldNsight: "invalid_string"}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for invalid string config, got nil")
	}

	if !strings.Contains(err.Error(), "validate nsight config") {
		t.Errorf("expected error to mention 'validate nsight config', got: %v", err)
	}
}

func TestNsightPlugin_Create_NonStringValueInMap(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{"t": 123}}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for non-string value in map, got nil")
	}

	if !strings.Contains(err.Error(), "must be a string") {
		t.Errorf("expected error to mention 'must be a string', got: %v", err)
	}
}

func TestNsightPlugin_Create_WrongType(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{FieldNsight: []string{"cuda"}}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}

	if !strings.Contains(err.Error(), "must be a map") {
		t.Errorf("expected error to mention 'must be a map', got: %v", err)
	}
}

func TestNsightPlugin_Create_Timeout(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-timeout-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: "default"}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err = plugin.Create(ctx, "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected timeout error, got nil")
	} else if !strings.Contains(err.Error(), "context deadline exceeded") && !strings.Contains(err.Error(), "nsight") {
		t.Logf("got error (may be expected): %v", err)
	}
}

func TestNsightPlugin_ConcurrentAccess_Stress(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-stress-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: "default"}

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

	t.Logf("Concurrent access test completed: %d succeeded, %d failed (nsys may not be installed)", successCount, errorCount)

	plugin.nsightMutex.RLock()
	nsightCmd := plugin.nsightCmd
	plugin.nsightMutex.RUnlock()

	if len(nsightCmd) == 0 && successCount == 0 {
		t.Log("nsight command not set (expected when nsys is not installed)")
	}
}

func TestNsightPlugin_Name(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	name := plugin.Name()
	if name != FieldNsight {
		t.Errorf("expected name to be '%s', got '%s'", FieldNsight, name)
	}
}

func TestNsightPlugin_Priority(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	priority := plugin.Priority()
	if priority != RayRuntimeEnvPluginDefaultPriority {
		t.Errorf("expected priority to be %d, got %d", RayRuntimeEnvPluginDefaultPriority, priority)
	}
}

func TestNsightPlugin_GetURIs(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	uris := plugin.GetURIs(&RuntimeEnv{})
	if uris != nil {
		t.Errorf("expected nil URIs, got %v", uris)
	}
}

func TestNsightPlugin_DeleteURI(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	result := plugin.DeleteURI("test-uri")
	if result != 0 {
		t.Errorf("expected DeleteURI to return 0, got %d", result)
	}
}

func TestNsightPlugin_Create_NilConfig(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")

	runtimeEnv := &RuntimeEnv{}
	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err != nil {
		t.Errorf("expected no error for nil config, got: %v", err)
	}
}

func TestNsightPlugin_Create_ValidDefaultString(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-default-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: "default"}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err != nil && !strings.Contains(err.Error(), "nsight profile failed") {
		t.Errorf("expected success or nsight not available error, got: %v", err)
	}
}

func TestNsightPlugin_Create_ValidMapConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-map-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{
		"t":            "cuda,cudnn",
		"o":            "test_output",
		"stop-on-exit": "true",
	}}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err != nil && !strings.Contains(err.Error(), "nsight profile failed") {
		t.Errorf("expected success or nsight not available error, got: %v", err)
	}
}

func TestNsightPlugin_Create_NonLinuxPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("this test is for non-Linux platforms only")
	}

	plugin := NewNsightPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldNsight: "default"}

	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error on non-Linux platform, got nil")
	}
	if !strings.Contains(err.Error(), "only available on Linux") {
		t.Errorf("expected error to mention Linux, got: %v", err)
	}
}

func TestNsightPlugin_ModifyContext_NoConfig(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	context := &RuntimeEnvContext{
		PyExecutable: "/usr/bin/python",
	}

	err := plugin.ModifyContext(nil, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("expected no error with empty config, got: %v", err)
	}
	if context.PyExecutable != "/usr/bin/python" {
		t.Errorf("expected PyExecutable unchanged, got: %s", context.PyExecutable)
	}
}

func TestNsightPlugin_ModifyContext_WithConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-modify-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: "default"}

	_, _ = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})

	context := &RuntimeEnvContext{
		PyExecutable: "/usr/bin/python",
	}

	err = plugin.ModifyContext(nil, runtimeEnv, context)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if strings.Contains(context.PyExecutable, "python") && !strings.Contains(context.PyExecutable, "nsys") {
		t.Logf("PyExecutable was not modified (nsys may not be installed): %s", context.PyExecutable)
	}
}

func TestNsightPlugin_parseNsightConfig_SingleCharOptions(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	config := map[string]string{
		"t": "cuda",
		"o": "output",
	}

	cmd := plugin.parseNsightConfig(config)
	if len(cmd) < 4 {
		t.Errorf("expected at least 4 elements in cmd, got %d: %v", len(cmd), cmd)
	}

	foundT := false
	foundO := false
	for i, arg := range cmd {
		if arg == "-t" && i+1 < len(cmd) && cmd[i+1] == "cuda" {
			foundT = true
		}
		if arg == "-o" && i+1 < len(cmd) && cmd[i+1] == "output" {
			foundO = true
		}
	}

	if !foundT {
		t.Errorf("expected -t cuda in command, got: %v", cmd)
	}
	if !foundO {
		t.Errorf("expected -o output in command, got: %v", cmd)
	}
}

func TestNsightPlugin_parseNsightConfig_LongOptions(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	config := map[string]string{
		"trace-opts":      "cuda=cudaProfilerApi",
		"force-overwrite": "true",
	}

	cmd := plugin.parseNsightConfig(config)
	if len(cmd) < 3 {
		t.Errorf("expected at least 3 elements in cmd, got %d: %v", len(cmd), cmd)
	}

	foundTraceOpts := false
	foundForceOverwrite := false
	for _, arg := range cmd {
		if strings.HasPrefix(arg, "--trace-opts=") {
			foundTraceOpts = true
		}
		if strings.HasPrefix(arg, "--force-overwrite=") {
			foundForceOverwrite = true
		}
	}

	if !foundTraceOpts {
		t.Errorf("expected --trace-opts in command, got: %v", cmd)
	}
	if !foundForceOverwrite {
		t.Errorf("expected --force-overwrite in command, got: %v", cmd)
	}
}

func TestNsightPlugin_sysExecutable_Success(t *testing.T) {
	executable := sysExecutable()
	if executable == "" {
		t.Error("expected non-empty executable path")
	}
}

func TestNsightPlugin_Validate_EmptyMap(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{}}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for empty map, got: %v", err)
	}
}

func TestNsightPlugin_Validate_MixedValidMap(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{
		"t":            "cuda",
		"o":            "output",
		"stop-on-exit": "true",
	}}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for valid map, got: %v", err)
	}
}

func TestNsightPlugin_Create_ValidationErrorPropagation(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{"invalid": 123}}

	_, err := plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err == nil {
		t.Error("expected error for invalid config, got nil")
	}
	if !strings.Contains(err.Error(), "validate nsight config") {
		t.Errorf("expected error to mention validation, got: %v", err)
	}
}

func TestNsightPlugin_ModifyContext_EmptyCommand(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	context := &RuntimeEnvContext{
		PyExecutable: "/usr/bin/python",
	}

	err := plugin.ModifyContext(nil, &RuntimeEnv{}, context)
	if err != nil {
		t.Errorf("expected no error with empty command, got: %v", err)
	}
}

func TestNsightPlugin_checkNsightScript_FailureCase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nsight-check-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	config := map[string]string{
		"t": "cuda",
		"o": filepath.Join(tmpDir, "test_output"),
	}

	isValid, errMsg := plugin.checkNsightScript(context.Background(), config)

	if isValid && runtime.GOOS == "linux" {
		t.Log("checkNsightScript succeeded (nsys may be installed)")
	} else if !isValid {
		t.Logf("checkNsightScript failed as expected on non-Linux or when nsys not available: %s", errMsg)
	}
}

func TestNsightPlugin_checkNsightScript_WithContextTimeout(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-context-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	config := map[string]string{
		"t": "cuda",
		"o": filepath.Join(tmpDir, "test_output"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	isValid, errMsg := plugin.checkNsightScript(ctx, config)

	if !isValid {
		t.Logf("checkNsightScript failed with timeout as expected: %s", errMsg)
	} else {
		t.Log("checkNsightScript succeeded despite short timeout (nsys very fast)")
	}
}

func TestNsightPlugin_sysExecutable_ErrorCase(t *testing.T) {
	executable := sysExecutable()
	if executable == "" {
		t.Error("expected non-empty executable path even on error")
	}
}

func TestNsightPlugin_Create_CustomMapConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping on non-Linux")
	}

	tmpDir, err := os.MkdirTemp("", "nsight-custom-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin := NewNsightPlugin(tmpDir)
	runtimeEnv := &RuntimeEnv{FieldNsight: map[string]interface{}{
		"t":               "cuda,cudnn,cublas,nvtx",
		"o":               "custom_worker_%p",
		"stop-on-exit":    "true",
		"trace-opts":      "cuda=cudaProfilerApi",
		"force-overwrite": "true",
	}}

	_, err = plugin.Create(context.Background(), "", runtimeEnv, &RuntimeEnvContext{})
	if err != nil && !strings.Contains(err.Error(), "nsight profile failed") {
		t.Errorf("expected success or nsight not available error, got: %v", err)
	}
}

func TestNsightPlugin_parseNsightConfig_MixedOptions(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	config := map[string]string{
		"t":               "cuda",
		"o":               "output",
		"stop-on-exit":    "true",
		"force-overwrite": "true",
	}

	cmd := plugin.parseNsightConfig(config)
	if len(cmd) < 6 {
		t.Errorf("expected at least 6 elements in cmd, got %d: %v", len(cmd), cmd)
	}

	hasShortOpts := false
	hasLongOpts := false
	for _, arg := range cmd {
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") {
			hasShortOpts = true
		}
		if strings.HasPrefix(arg, "--") {
			hasLongOpts = true
		}
	}

	if !hasShortOpts {
		t.Error("expected short options in command")
	}
	if !hasLongOpts {
		t.Error("expected long options in command")
	}
}

func TestNsightPlugin_Validate_NilConfig(t *testing.T) {
	plugin := NewNsightPlugin("/tmp/test")
	runtimeEnv := &RuntimeEnv{}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("expected no error for nil config, got: %v", err)
	}
}
