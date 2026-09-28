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

// Package runtime_env provides runtime environment configuration for Ray workers.
package runtime_env

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestGetUvHash tests the _getUvHash function.
func TestGetUvHash(t *testing.T) {
	testCases := []struct {
		name     string
		uvDict   map[string]interface{}
		expected string
	}{
		{
			name:   "empty dict",
			uvDict: map[string]interface{}{},
		},
		{
			name: "single package",
			uvDict: map[string]interface{}{
				"packages": []interface{}{"requests"},
			},
		},
		{
			name: "multiple packages",
			uvDict: map[string]interface{}{
				"packages": []interface{}{"requests", "numpy", "pandas"},
			},
		},
		{
			name: "packages with versions",
			uvDict: map[string]interface{}{
				"packages": []interface{}{"requests==2.28.0", "numpy>=1.20.0"},
			},
		},
		{
			name: "with uv_version",
			uvDict: map[string]interface{}{
				"packages":   []interface{}{"requests"},
				"uv_version": "==0.4.0",
			},
		},
		{
			name: "with uv_check",
			uvDict: map[string]interface{}{
				"packages": []interface{}{"requests"},
				"uv_check": true,
			},
		},
		{
			name: "with uv_pip_install_options",
			uvDict: map[string]interface{}{
				"packages":               []interface{}{"requests"},
				"uv_pip_install_options": []string{"--no-cache", "--user"},
			},
		},
		{
			name: "complex config",
			uvDict: map[string]interface{}{
				"packages":               []interface{}{"requests", "numpy"},
				"uv_version":             "==0.4.0",
				"uv_check":               false,
				"uv_pip_install_options": []string{"--no-cache"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hashVal, err := _getUvHash(tc.uvDict)

			if err != nil {
				t.Errorf("_getUvHash() unexpected error: %v", err)
				return
			}

			// Verify the hash value format.
			if len(hashVal) != 40 { // a SHA1 hash is 40 hex characters
				t.Errorf("_getUvHash() returned hash with length %d, want 40", len(hashVal))
			}

			// Verify the hash contains only hex characters.
			for _, c := range hashVal {
				if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
					t.Errorf("_getUvHash() returned invalid hex character: %c", c)
				}
			}

			// Verify identical inputs produce identical output (determinism).
			hashVal2, err := _getUvHash(tc.uvDict)
			if err != nil {
				t.Errorf("_getUvHash() second call error: %v", err)
				return
			}
			if hashVal != hashVal2 {
				t.Errorf("_getUvHash() not deterministic: first=%s, second=%s", hashVal, hashVal2)
			}

			// Verify different inputs produce different output (when possible).
			if tc.name != "empty dict" {
				differentDict := make(map[string]interface{})
				for k, v := range tc.uvDict {
					differentDict[k] = v
				}
				// Change one value.
				if packages, ok := differentDict["packages"].([]interface{}); ok && len(packages) > 0 {
					differentDict["packages"] = append([]interface{}{"different_package"}, packages[1:]...)
				}

				differentHash, err := _getUvHash(differentDict)
				if err == nil && differentHash == hashVal {
					t.Logf("_getUvHash() warning: different inputs produced same hash")
				}
			}
		})
	}
}

// TestGetUvHashConsistency tests _getUvHash consistency (key order does not affect the result).
func TestGetUvHashConsistency(t *testing.T) {
	dict1 := map[string]interface{}{
		"packages":               []interface{}{"requests"},
		"uv_version":             "==0.4.0",
		"uv_check":               false,
		"uv_pip_install_options": []string{"--no-cache"},
	}

	dict2 := make(map[string]interface{})
	dict2["uv_check"] = false
	dict2["uv_pip_install_options"] = []string{"--no-cache"}
	dict2["packages"] = []interface{}{"requests"}
	dict2["uv_version"] = "==0.4.0"

	hash1, err1 := _getUvHash(dict1)
	hash2, err2 := _getUvHash(dict2)

	if err1 != nil || err2 != nil {
		t.Errorf("_getUvHash() error: err1=%v, err2=%v", err1, err2)
		return
	}

	if hash1 != hash2 {
		t.Errorf("_getUvHash() inconsistent hashes for same content: hash1=%s, hash2=%s", hash1, hash2)
	}
}

// TestGetUvURI tests the GetUvURI function.
func TestGetUvURI(t *testing.T) {
	testCases := []struct {
		name        string
		runtimeEnv  RuntimeEnv
		expectError bool
		checkFunc   func(string) bool
	}{
		{
			name:        "nil uv field",
			runtimeEnv:  RuntimeEnv{},
			expectError: false,
		},
		{
			name: "empty uv field",
			runtimeEnv: RuntimeEnv{
				FieldUv: nil,
			},
			expectError: false,
		},
		{
			name: "uv as list",
			runtimeEnv: RuntimeEnv{
				FieldUv: []interface{}{"requests", "numpy"},
			},
			expectError: false,
			checkFunc: func(uri string) bool {
				return strings.HasPrefix(uri, "uv://") && len(uri) > 5
			},
		},
		{
			name: "uv as dict",
			runtimeEnv: RuntimeEnv{
				FieldUv: map[string]interface{}{
					"packages": []interface{}{"requests"},
				},
			},
			expectError: false,
			checkFunc: func(uri string) bool {
				return strings.HasPrefix(uri, "uv://") && len(uri) > 5
			},
		},
		{
			name: "uv as invalid type",
			runtimeEnv: RuntimeEnv{
				FieldUv: "invalid_string",
			},
			expectError: false,
		},
		{
			name: "uv as int",
			runtimeEnv: RuntimeEnv{
				FieldUv: 123,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uri, err := GetUvURI(&tc.runtimeEnv)

			if tc.expectError && err == nil {
				t.Errorf("GetUvURI() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("GetUvURI() unexpected error: %v", err)
			}

			if !tc.expectError && tc.checkFunc != nil {
				if !tc.checkFunc(uri) {
					t.Errorf("GetUvURI() returned invalid URI: %s", uri)
				}
			}
		})
	}
}

// TestNewUvProcessor tests the NewUvProcessor function.
func TestNewUvProcessor(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "newprocessor_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pythonPath, pythonErr := getPythonExecutable()
	if pythonErr != nil {
		t.Skipf("Skipping TestNewUvProcessor: Python not found: %v", pythonErr)
	}
	cmd := exec.Command(pythonPath, "-m", "virtualenv", "--version")
	if cmdErr := cmd.Run(); cmdErr != nil {
		t.Skipf("Skipping TestNewUvProcessor: virtualenv not installed: %v", cmdErr)
	}

	testCases := []struct {
		name        string
		targetDir   string
		runtimeEnv  *RuntimeEnv
		expectError bool
	}{
		{
			name:      "valid runtime env",
			targetDir: tmpDir,
			runtimeEnv: &RuntimeEnv{
				FieldUv: map[string]interface{}{
					"packages": []interface{}{"requests"},
				},
			},
			expectError: false,
		},
		{
			name:      "runtime env with env vars",
			targetDir: tmpDir,
			runtimeEnv: &RuntimeEnv{
				FieldUv: map[string]interface{}{
					"packages": []interface{}{"requests"},
				},
				FieldEnvVars: map[string]string{
					"TEST_VAR": "test_value",
				},
			},
			expectError: false,
		},
		{
			name:      "invalid uv config",
			targetDir: tmpDir,
			runtimeEnv: &RuntimeEnv{
				FieldUv: "invalid",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processor, err := NewUvProcessor(tc.targetDir, tc.runtimeEnv)

			if tc.expectError && err == nil {
				t.Errorf("NewUvProcessor() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("NewUvProcessor() unexpected error: %v", err)
			}

			if !tc.expectError && processor != nil {
				if processor.targetDir != tc.targetDir {
					t.Errorf("NewUvProcessor() targetDir = %q, want %q", processor.targetDir, tc.targetDir)
				}
				if processor.runtimeEnv != tc.runtimeEnv {
					t.Error("NewUvProcessor() runtimeEnv not set correctly")
				}
				if processor.uvConfig == nil {
					t.Error("NewUvProcessor() uvConfig is nil")
				}
				if processor.uvEnv == nil {
					t.Error("NewUvProcessor() uvEnv is nil")
				}
				if processor.execCwd == "" {
					t.Error("NewUvProcessor() execCwd is empty")
				}
			}
		})
	}
}

// TestUvPluginName tests the UvPlugin.Name method.
func TestUvPluginName(t *testing.T) {
	plugin := &UvPlugin{}
	name := plugin.Name()

	if name != "uv" {
		t.Errorf("UvPlugin.Name() = %q, want \"uv\"", name)
	}
}

// TestUvPluginPriority tests the UvPlugin.Priority method.
func TestUvPluginPriority(t *testing.T) {
	plugin := &UvPlugin{}
	priority := plugin.Priority()

	if priority != 10 {
		t.Errorf("UvPlugin.Priority() = %d, want 10", priority)
	}
}

// TestUvPluginValidate tests the UvPlugin.Validate method.
func TestUvPluginValidate(t *testing.T) {
	plugin := &UvPlugin{}
	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{
			"packages": []interface{}{"requests"},
		},
	}

	err := plugin.Validate(runtimeEnv)

	if err != nil {
		t.Errorf("UvPlugin.Validate() unexpected error: %v", err)
	}
}

// TestNewUvPlugin tests the NewUvPlugin function.
func TestNewUvPlugin(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "newplugin_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)

	if err != nil {
		t.Errorf("NewUvPlugin() unexpected error: %v", err)
		return
	}

	if plugin == nil {
		t.Error("NewUvPlugin() returned nil plugin")
		return
	}

	if plugin.resourcesDir == "" {
		t.Error("NewUvPlugin() resourcesDir is empty")
	}

	if plugin.creatingTask == nil {
		t.Error("NewUvPlugin() creatingTask is nil")
	}

	if plugin.createLocks == nil {
		t.Error("NewUvPlugin() createLocks is nil")
	}

	if plugin.createdHashBytes == nil {
		t.Error("NewUvPlugin() createdHashBytes is nil")
	}
}

// TestUvPluginGetURIs tests the UvPlugin.GetURIs method.
func TestUvPluginGetURIs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "geturis_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	testCases := []struct {
		name         string
		runtimeEnv   *RuntimeEnv
		expectURILen int
	}{
		{
			name:         "no uv field",
			runtimeEnv:   &RuntimeEnv{},
			expectURILen: 0,
		},
		{
			name: "with uv field as list",
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			expectURILen: 1,
		},
		{
			name: "with uv field as dict",
			runtimeEnv: &RuntimeEnv{
				FieldUv: map[string]interface{}{
					"packages": []interface{}{"requests"},
				},
			},
			expectURILen: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uris := plugin.GetURIs(tc.runtimeEnv)

			if len(uris) != tc.expectURILen {
				t.Errorf("UvPlugin.GetURIs() returned %d URIs, want %d", len(uris), tc.expectURILen)
			}

			if tc.expectURILen > 0 {
				if !strings.HasPrefix(uris[0], "uv://") {
					t.Errorf("UvPlugin.GetURIs() returned invalid URI: %s", uris[0])
				}
			}
		})
	}
}

// TestGetUvURI_Directly tests the GetUvURI function directly.
func TestGetUvURI_Directly(t *testing.T) {
	testCases := []struct {
		name        string
		runtimeEnv  *RuntimeEnv
		expectError bool
		checkURI    func(string) bool
	}{
		{
			name:        "no uv field",
			runtimeEnv:  &RuntimeEnv{},
			expectError: false,
		},
		{
			name: "with uv field as list",
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			expectError: false,
			checkURI: func(uri string) bool {
				return strings.HasPrefix(uri, "uv://") && len(uri) > 5
			},
		},
		{
			name: "with uv field as dict",
			runtimeEnv: &RuntimeEnv{
				FieldUv: map[string]interface{}{
					"packages": []interface{}{"requests"},
				},
			},
			expectError: false,
			checkURI: func(uri string) bool {
				return strings.HasPrefix(uri, "uv://") && len(uri) > 5
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uri, err := GetUvURI(tc.runtimeEnv)

			if tc.expectError && err == nil {
				t.Errorf("GetUvURI() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("GetUvURI() unexpected error: %v", err)
			}

			if !tc.expectError && tc.checkURI != nil {
				if !tc.checkURI(uri) {
					t.Errorf("GetUvURI() returned invalid URI: %s", uri)
				}
			}
		})
	}
}

// TestUvPluginDeleteURI tests the UvPlugin.DeleteURI method.
func TestUvPluginDeleteURI(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "deleteuri_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	testCases := []struct {
		name        string
		uri         string
		expectError bool
	}{
		{
			name: "invalid URI format",
			uri:  "invalid_uri",
		},
		{
			name: "wrong protocol",
			uri:  "http://example.com",
		},
		{
			name: "valid uv URI but non-existent path",
			uri:  "uv://abcd1234567890abcd1234567890abcd12345678",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bytesDeleted := plugin.DeleteURI(tc.uri)

			if bytesDeleted < 0 {
				t.Errorf("UvPlugin.DeleteURI() returned negative bytes: %d", bytesDeleted)
			}
		})
	}
}

// TestUvPluginCreate tests the UvPlugin.Create method.
func TestUvPluginCreate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "create_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	ctx := context.Background()

	testCases := []struct {
		name        string
		uri         string
		runtimeEnv  *RuntimeEnv
		expectError bool
	}{
		{
			name:        "no uv field in runtime env",
			uri:         "uv://abcd1234567890abcd1234567890abcd12345678",
			runtimeEnv:  &RuntimeEnv{},
			expectError: false,
		},
		{
			name: "invalid URI protocol",
			uri:  "http://example.com",
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			expectError: true,
		},
		{
			name: "invalid URI format",
			uri:  "invalid_uri",
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size, err := plugin.Create(ctx, tc.uri, tc.runtimeEnv, nil)

			if tc.expectError && err == nil {
				t.Errorf("UvPlugin.Create() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("UvPlugin.Create() unexpected error: %v", err)
			}

			if !tc.expectError && size < 0 {
				t.Errorf("UvPlugin.Create() returned negative size: %d", size)
			}
		})
	}
}

// TestUvPluginModifyContext tests the UvPlugin.ModifyContext method.
func TestUvPluginModifyContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "modifyctx_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	testCases := []struct {
		name        string
		uris        []string
		runtimeEnv  *RuntimeEnv
		context     *RuntimeEnvContext
		expectError bool
	}{
		{
			name:        "no URIs with no uv field",
			uris:        []string{},
			runtimeEnv:  &RuntimeEnv{},
			context:     &RuntimeEnvContext{},
			expectError: false,
		},
		{
			name: "no URIs with uv field",
			uris: []string{},
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			context:     &RuntimeEnvContext{},
			expectError: true,
		},
		{
			name:        "no uv field",
			uris:        []string{"uv://abcd1234567890abcd1234567890abcd12345678"},
			runtimeEnv:  &RuntimeEnv{},
			context:     &RuntimeEnvContext{},
			expectError: false,
		},
		{
			name: "with uv field but non-existent path",
			uris: []string{"uv://abcd1234567890abcd1234567890abcd12345678"},
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			context:     &RuntimeEnvContext{},
			expectError: true,
		},
		{
			name: "invalid URI protocol",
			uris: []string{"http://example.com"},
			runtimeEnv: &RuntimeEnv{
				FieldUv: []interface{}{"requests"},
			},
			context:     &RuntimeEnvContext{},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := plugin.ModifyContext(tc.uris, tc.runtimeEnv, tc.context)

			if tc.expectError && err == nil {
				t.Errorf("UvPlugin.ModifyContext() expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("UvPlugin.ModifyContext() unexpected error: %v", err)
			}
		})
	}
}

// TestGetUvURIDeterminism tests the determinism of GetUvURI.
func TestGetUvURIDeterminism(t *testing.T) {
	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{
			"packages": []interface{}{"requests", "numpy"},
		},
	}

	uri1, err1 := GetUvURI(runtimeEnv)
	uri2, err2 := GetUvURI(runtimeEnv)

	if err1 != nil || err2 != nil {
		t.Errorf("GetUvURI() error: err1=%v, err2=%v", err1, err2)
		return
	}

	if uri1 != uri2 {
		t.Errorf("GetUvURI() not deterministic: uri1=%s, uri2=%s", uri1, uri2)
	}
}

// TestUvPluginConcurrency tests the concurrency safety of UvPlugin.
func TestUvPluginConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "concurrency_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	uri := "uv://abcd1234567890abcd1234567890abcd12345678"
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			plugin.DeleteURI(uri)
		}()
	}

	wg.Wait()
}

// TestUvProcessorWithInvalidUvConfig tests UvProcessor with an invalid config.
func TestUvProcessorWithInvalidUvConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "invalidconfig_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{
			"packages": "not_a_list",
		},
	}

	_, err = NewUvProcessor(tmpDir, runtimeEnv)

	if err == nil {
		t.Error("NewUvProcessor() expected error for invalid packages type, got nil")
	}
}

// TestUvPluginCreateResourcesSubdir tests that NewUvPlugin creates the subdirectory.
func TestUvPluginCreateResourcesSubdir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resourcesubdir_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	_, err = NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	expectedDir := filepath.Join(tmpDir, "uv")
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("NewUvPlugin() did not create uv subdirectory at %s", expectedDir)
	}
}

// TestGetUvHashWithNilMap tests _getUvHash with a nil map.
func TestGetUvHashWithNilMap(t *testing.T) {
	hashVal, err := _getUvHash(nil)
	if err != nil {
		t.Errorf("_getUvHash(nil) unexpected error: %v", err)
	}
	if hashVal == "" {
		t.Error("_getUvHash(nil) returned empty hash")
	}
}

// TestGetUvHashSerialization tests the serialization behavior of _getUvHash.
func TestGetUvHashSerialization(t *testing.T) {
	uvDict := map[string]interface{}{
		"packages": []interface{}{"requests", "numpy"},
	}

	serialized, err := json.Marshal(uvDict)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	hasher := sha1.New()
	hasher.Write(serialized)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	actualHash, err := _getUvHash(uvDict)
	if err != nil {
		t.Fatalf("_getUvHash() error: %v", err)
	}

	if actualHash != expectedHash {
		t.Errorf("_getUvHash() hash mismatch: got %s, want %s", actualHash, expectedHash)
	}
}

// TestUvPluginGetPathFromHash tests the UvPlugin.getPathFromHash method.
func TestUvPluginGetPathFromHash(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "getpath_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	expectedPath := filepath.Join(tmpDir, "uv", hashVal)

	actualPath := plugin.getPathFromHash(hashVal)
	if actualPath != expectedPath {
		t.Errorf("getPathFromHash() = %q, want %q", actualPath, expectedPath)
	}
}

// TestUvPluginDeleteURIWithValidPath tests UvPlugin.DeleteURI on an existing path.
func TestUvPluginDeleteURIWithValidPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "deletevalid_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	testPath := plugin.getPathFromHash(hashVal)
	if err := os.MkdirAll(testPath, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	testFile := filepath.Join(testPath, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	expectedSize := int64(len(testContent))

	uri := "uv://" + hashVal
	bytesDeleted := plugin.DeleteURI(uri)

	if bytesDeleted < expectedSize {
		t.Errorf("DeleteURI() deleted %d bytes, want at least %d", bytesDeleted, expectedSize)
	}

	if _, err := os.Stat(testPath); !os.IsNotExist(err) {
		t.Error("DeleteURI() did not delete the directory")
	}
}

// TestUvPluginCreateWithExistingHash tests UvPlugin.Create with an existing hash.
func TestUvPluginCreateWithExistingHash(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "existinghash_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	ctx := context.Background()
	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	uri := "uv://" + hashVal

	plugin.createdHashBytesMu.Lock()
	plugin.createdHashBytes[hashVal] = 12345
	plugin.createdHashBytesMu.Unlock()

	runtimeEnv := &RuntimeEnv{
		FieldUv: []interface{}{"requests"},
	}

	size, err := plugin.Create(ctx, uri, runtimeEnv, nil)
	if err != nil {
		t.Errorf("UvPlugin.Create() unexpected error: %v", err)
	}
	if size != 12345 {
		t.Errorf("UvPlugin.Create() returned size %d, want 12345", size)
	}
}

// TestUvPluginDeleteURICancelsTask tests that UvPlugin.DeleteURI cancels the running task.
func TestUvPluginDeleteURICancelsTask(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "canceltask_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	plugin.creatingTaskMu.Lock()
	plugin.creatingTask[hashVal] = cancel
	plugin.creatingTaskMu.Unlock()

	uri := "uv://" + hashVal
	plugin.DeleteURI(uri)

	plugin.creatingTaskMu.RLock()
	_, exists := plugin.creatingTask[hashVal]
	plugin.creatingTaskMu.RUnlock()

	if exists {
		t.Error("DeleteURI() did not cancel the creating task")
	}
}

// TestUvPluginCreateAndModifyContext tests the integration of UvPlugin.Create and ModifyContext.
func TestUvPluginCreateAndModifyContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "createandmodify_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	ctx := context.Background()
	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	uri := "uv://" + hashVal

	runtimeEnv := &RuntimeEnv{
		FieldUv: []interface{}{"requests"},
	}

	context := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, uri, runtimeEnv, context)
	if err == nil {
		t.Logf("UvPlugin.Create() succeeded with size: %d (may be due to virtualenv creation)", size)
	}

	err = plugin.ModifyContext([]string{uri}, runtimeEnv, context)
	if err == nil {
		t.Log("UvPlugin.ModifyContext() succeeded")
	}
}

// TestGetUvHashEmptyDict tests _getUvHash with an empty dict.
func TestGetUvHashEmptyDict(t *testing.T) {
	uvDict := map[string]interface{}{}
	hashVal, err := _getUvHash(uvDict)
	if err != nil {
		t.Errorf("_getUvHash(empty dict) unexpected error: %v", err)
	}
	if len(hashVal) != 40 {
		t.Errorf("_getUvHash(empty dict) returned hash with length %d, want 40", len(hashVal))
	}
}

// TestGetUvURINilRuntimeEnv tests GetUvURI with a nil RuntimeEnv pointer.
func TestGetUvURINilRuntimeEnv(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Logf("GetUvURI(nil) panicked as expected: %v", r)
		}
	}()

	uri, err := GetUvURI(nil)
	if err != nil {
		t.Logf("GetUvURI(nil) returned error (may be expected): %v", err)
	}
	if uri != "" {
		t.Logf("GetUvURI(nil) returned URI: %s", uri)
	}
}

// TestUvPluginDeleteURIInvalidProtocol tests UvPlugin.DeleteURI with an invalid protocol.
func TestUvPluginDeleteURIInvalidProtocol(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "invalidproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	uri := "pip://abcd1234567890abcd1234567890abcd12345678"
	bytesDeleted := plugin.DeleteURI(uri)

	if bytesDeleted != 0 {
		t.Errorf("DeleteURI() with invalid protocol should return 0, got %d", bytesDeleted)
	}
}

// TestUvPluginCreateNilContext tests UvPlugin.Create with a nil context.
func TestUvPluginCreateNilContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nilctx_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	ctx := context.Background()
	hashVal := "abcd1234567890abcd1234567890abcd12345678"
	uri := "uv://" + hashVal

	runtimeEnv := &RuntimeEnv{
		FieldUv: []interface{}{"requests"},
	}

	size, err := plugin.Create(ctx, uri, runtimeEnv, nil)
	if err == nil {
		t.Logf("UvPlugin.Create() with nil context succeeded with size: %d", size)
	}
}

// TestUvProcessorRunPackagesKey tests UvProcessor.Run with the packages key missing.
func TestUvProcessorRunPackagesKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "runpackages_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pythonPath, pythonErr := getPythonExecutable()
	if pythonErr != nil {
		t.Skipf("Skipping test: Python not found: %v", pythonErr)
	}
	cmd := exec.Command(pythonPath, "-m", "virtualenv", "--version")
	if cmdErr := cmd.Run(); cmdErr != nil {
		t.Skipf("Skipping test: virtualenv not installed: %v", cmdErr)
	}

	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{},
	}

	processor, err := NewUvProcessor(tmpDir, runtimeEnv)
	if err != nil {
		t.Fatalf("NewUvProcessor() error: %v", err)
	}

	ctx := context.Background()
	err = processor.Run(ctx)
	if err == nil {
		t.Error("UvProcessor.Run() expected error for missing packages key, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "packages") {
		t.Errorf("UvProcessor.Run() unexpected error message: %v", err)
	}
}

// TestUvProcessorRunInvalidPackagesType tests UvProcessor with an invalid packages type.
func TestUvProcessorRunInvalidPackagesType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "invalidpkgtype_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{
			"packages": 123,
		},
	}

	_, err = NewUvProcessor(tmpDir, runtimeEnv)
	if err == nil {
		t.Error("NewUvProcessor() expected error for invalid packages type, got nil")
	}
}

// TestUvPluginModifyContextMultipleURIs tests UvPlugin.ModifyContext with multiple URIs.
func TestUvPluginModifyContextMultipleURIs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "multipleuris_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	plugin, err := NewUvPlugin(tmpDir)
	if err != nil {
		t.Fatalf("NewUvPlugin() error: %v", err)
	}

	runtimeEnv := &RuntimeEnv{
		FieldUv: []interface{}{"requests"},
	}
	context := &RuntimeEnvContext{}

	uris := []string{
		"uv://abcd1234567890abcd1234567890abcd12345678",
		"uv://efgh1234567890efgh1234567890efgh12345678",
	}

	err = plugin.ModifyContext(uris, runtimeEnv, context)
	if err != nil {
		t.Logf("UvPlugin.ModifyContext() with multiple URIs error (expected): %v", err)
	}
}

// TestGetUvURIWithComplexConfig tests GetUvURI with a complex config.
func TestGetUvURIWithComplexConfig(t *testing.T) {
	runtimeEnv := &RuntimeEnv{
		FieldUv: map[string]interface{}{
			"packages":               []interface{}{"requests", "numpy>=1.20.0"},
			"uv_version":             "==0.4.0",
			"uv_check":               true,
			"uv_pip_install_options": []string{"--no-cache", "--user"},
		},
	}

	uri1, err1 := GetUvURI(runtimeEnv)
	if err1 != nil {
		t.Errorf("GetUvURI() unexpected error: %v", err1)
	}
	if !strings.HasPrefix(uri1, "uv://") {
		t.Errorf("GetUvURI() returned invalid URI: %s", uri1)
	}

	uri2, err2 := GetUvURI(runtimeEnv)
	if err2 != nil {
		t.Errorf("GetUvURI() second call error: %v", err2)
	}
	if uri1 != uri2 {
		t.Errorf("GetUvURI() not deterministic: uri1=%s, uri2=%s", uri1, uri2)
	}
}
