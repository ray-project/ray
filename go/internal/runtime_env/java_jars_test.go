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
	"testing"
)

// TestJavaJarsPlugin_Name tests the plugin name.
func TestJavaJarsPlugin_Name(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	if plugin.Name() != "java_jars" {
		t.Errorf("Expected name 'java_jars', got '%s'", plugin.Name())
	}
}

// TestJavaJarsPlugin_Priority tests the plugin priority.
func TestJavaJarsPlugin_Priority(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	if priority := plugin.Priority(); priority != 10 {
		t.Errorf("Expected priority 10, got %d", priority)
	}
}

// TestJavaJarsPlugin_Validate_Empty tests validation of an empty config.
func TestJavaJarsPlugin_Validate_Empty(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for empty config, got %v", err)
	}
}

// TestJavaJarsPlugin_Validate_ValidArray tests validation of a valid string
// array.
func TestJavaJarsPlugin_Validate_ValidArray(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{
		"java_jars": []string{"https://s3.example.com/bucket/file.jar", "gcs://bucket/another.jar"},
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid array, got %v", err)
	}
}

// TestJavaJarsPlugin_Validate_ValidInterfaceArray tests validation of a valid
// interface array.
func TestJavaJarsPlugin_Validate_ValidInterfaceArray(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{
		"java_jars": []interface{}{"https://s3.example.com/bucket/file.jar", "gcs://bucket/another.jar"},
	}

	err := plugin.Validate(runtimeEnv)
	if err != nil {
		t.Errorf("Expected no error for valid interface array, got %v", err)
	}
}

// TestJavaJarsPlugin_Validate_InvalidType tests validation of an invalid type.
func TestJavaJarsPlugin_Validate_InvalidType(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{
		"java_jars": "not_an_array",
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for invalid type, got nil")
	}
}

// TestJavaJarsPlugin_Validate_NonStringElement tests validation with a
// non-string element.
func TestJavaJarsPlugin_Validate_NonStringElement(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{
		"java_jars": []interface{}{"valid.jar", 123},
	}

	err := plugin.Validate(runtimeEnv)
	if err == nil {
		t.Error("Expected error for non-string element, got nil")
	}
}

// TestJavaJarsPlugin_GetURIs_Empty tests GetURIs with an empty config.
func TestJavaJarsPlugin_GetURIs_Empty(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	runtimeEnv := &RuntimeEnv{}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != 0 {
		t.Errorf("Expected 0 URIs, got %d", len(uris))
	}
}

// TestJavaJarsPlugin_GetURIs_WithURIs tests GetURIs.
func TestJavaJarsPlugin_GetURIs_WithURIs(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	expectedURIs := []string{"https://s3.example.com/bucket/file1.jar", "gcs://bucket/file2.jar"}
	runtimeEnv := &RuntimeEnv{
		"java_jars": expectedURIs,
	}

	uris := plugin.GetURIs(runtimeEnv)
	if len(uris) != len(expectedURIs) {
		t.Errorf("Expected %d URIs, got %d", len(expectedURIs), len(uris))
	}
	for i, uri := range uris {
		if uri != expectedURIs[i] {
			t.Errorf("Expected URI[%d] = %s, got %s", i, expectedURIs[i], uri)
		}
	}
}

// TestJavaJarsPlugin_ModifyContext_Empty tests ModifyContext with empty URIs.
func TestJavaJarsPlugin_ModifyContext_Empty(t *testing.T) {
	plugin := &JavaJarsPlugin{}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	err := plugin.ModifyContext([]string{}, nil, context)
	if err != nil {
		t.Errorf("Expected no error for empty URIs, got %v", err)
	}
}

// TestJavaJarsPlugin_ModifyContext_SingleURI tests ModifyContext with a single
// URI.
func TestJavaJarsPlugin_ModifyContext_SingleURI(t *testing.T) {
	// Create a temporary JAR file.
	tmpDir := t.TempDir()
	jarFile := filepath.Join(tmpDir, "test.jar")
	if err := os.WriteFile(jarFile, []byte("fake jar content"), 0644); err != nil {
		t.Fatalf("Failed to create test JAR file: %v", err)
	}

	plugin := &JavaJarsPlugin{
		resourcesDir: tmpDir,
	}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	// Use a local-path URI (no protocol prefix).
	uris := []string{jarFile}
	err := plugin.ModifyContext(uris, nil, context)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	classpath, ok := context.EnvVars["CLASSPATH"]
	if !ok {
		t.Error("Expected CLASSPATH to be set")
	}
	if classpath != jarFile {
		t.Errorf("Expected CLASSPATH = %s, got %s", jarFile, classpath)
	}
}

// TestJavaJarsPlugin_ModifyContext_MultipleURIs tests ModifyContext with
// multiple URIs.
func TestJavaJarsPlugin_ModifyContext_MultipleURIs(t *testing.T) {
	// Create temporary JAR files.
	tmpDir := t.TempDir()
	jar1 := filepath.Join(tmpDir, "test1.jar")
	jar2 := filepath.Join(tmpDir, "test2.jar")
	if err := os.WriteFile(jar1, []byte("jar1"), 0644); err != nil {
		t.Fatalf("Failed to create test JAR 1: %v", err)
	}
	if err := os.WriteFile(jar2, []byte("jar2"), 0644); err != nil {
		t.Fatalf("Failed to create test JAR 2: %v", err)
	}

	plugin := &JavaJarsPlugin{
		resourcesDir: tmpDir,
	}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	uris := []string{jar1, jar2}
	err := plugin.ModifyContext(uris, nil, context)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	classpath, ok := context.EnvVars["CLASSPATH"]
	if !ok {
		t.Error("Expected CLASSPATH to be set")
	}

	// Both JAR files should appear in the CLASSPATH.
	expectedPath := jar1 + string(os.PathListSeparator) + jar2
	if classpath != expectedPath {
		t.Errorf("Expected CLASSPATH = %s, got %s", expectedPath, classpath)
	}
}

// TestJavaJarsPlugin_ModifyContext_AppendExisting tests appending to an
// existing CLASSPATH.
func TestJavaJarsPlugin_ModifyContext_AppendExisting(t *testing.T) {
	tmpDir := t.TempDir()
	jarFile := filepath.Join(tmpDir, "test.jar")
	if err := os.WriteFile(jarFile, []byte("fake jar"), 0644); err != nil {
		t.Fatalf("Failed to create test JAR file: %v", err)
	}

	plugin := &JavaJarsPlugin{
		resourcesDir: tmpDir,
	}
	context := &RuntimeEnvContext{
		EnvVars: map[string]string{
			"CLASSPATH": "/existing/path",
		},
	}

	uris := []string{jarFile}
	err := plugin.ModifyContext(uris, nil, context)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	classpath, ok := context.EnvVars["CLASSPATH"]
	if !ok {
		t.Error("Expected CLASSPATH to be set")
	}

	// The new JAR path should come first.
	expectedPath := jarFile + string(os.PathListSeparator) + "/existing/path"
	if classpath != expectedPath {
		t.Errorf("Expected CLASSPATH = %s, got %s", expectedPath, classpath)
	}
}

func TestJavaJarsPlugin_ModifyContext_NoValidation(t *testing.T) {
	plugin := &JavaJarsPlugin{
		resourcesDir: "/tmp",
	}
	context := &RuntimeEnvContext{
		EnvVars: make(map[string]string),
	}

	uris := []string{"/nonexistent/path/file.jar"}
	err := plugin.ModifyContext(uris, nil, context)
	if err != nil {
		t.Errorf("Expected no error for non-existent file, got %v", err)
	}

	classpath, ok := context.EnvVars["CLASSPATH"]
	if !ok {
		t.Error("Expected CLASSPATH to be set")
	}

	expectedPath := "/nonexistent/path/file.jar"
	if classpath != expectedPath {
		t.Errorf("Expected CLASSPATH=%s, got %s", expectedPath, classpath)
	}
}

// TestIsJarURI tests the IsJarURI function.
func TestIsJarURI(t *testing.T) {
	tests := []struct {
		uri      string
		expected bool
	}{
		{"https://s3.example.com/bucket/file.jar", true},
		{"gcs://bucket/file.JAR", true},
		{"https://example.com/lib.Jar", true},
		{"file:///path/to/file.jar", true},
		{"https://s3.example.com/bucket/file.zip", false},
		{"gcs://bucket/file.txt", false},
		{"", false},
	}

	for _, test := range tests {
		result := IsJarURI(test.uri)
		if result != test.expected {
			t.Errorf("IsJarURI(%q) = %v, expected %v", test.uri, result, test.expected)
		}
	}
}

// TestJavaJarsPlugin_needsRemoteDownload tests the needsRemoteDownload helper.
func TestJavaJarsPlugin_needsRemoteDownload(t *testing.T) {
	tests := []struct {
		uri      string
		expected bool
	}{
		{"gcs://bucket/file.jar", true},
		{"https://s3.example.com/bucket/file.jar", true},
		{"https://example.com/file.jar", true},
		{"http://example.com/file.jar", true},
		{"/local/path/file.jar", false},
		{"file:///local/file.jar", false},
		{"", false},
	}

	for _, test := range tests {
		result := needsRemoteDownload(test.uri)
		if result != test.expected {
			t.Errorf("needsRemoteDownload(%q) = %v, expected %v", test.uri, result, test.expected)
		}
	}
}

// TestNewJavaJarsPlugin tests the plugin constructor.
func TestNewJavaJarsPlugin(t *testing.T) {
	tmpDir := t.TempDir()
	gcsClient := &MockGCSClient{}

	plugin, err := NewJavaJarsPlugin(tmpDir, gcsClient)
	if err != nil {
		t.Fatal(err)
	}

	if plugin == nil {
		t.Fatal("Expected non-nil plugin")
	}
	if plugin.Name() != "java_jars" {
		t.Errorf("Expected name 'java_jars', got '%s'", plugin.Name())
	}
	if plugin.Priority() != 10 {
		t.Errorf("Expected priority 10, got %d", plugin.Priority())
	}
}

// TestJavaJarsPlugin_DeleteURI tests DeleteURI.
func TestJavaJarsPlugin_DeleteURI(t *testing.T) {
	tmpDir := t.TempDir()
	plugin := &JavaJarsPlugin{
		resourcesDir: tmpDir,
	}

	// Create a test file.
	testFile := filepath.Join(tmpDir, "test.jar")
	content := []byte("test content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Delete the file.
	size := plugin.DeleteURI(testFile)

	// The reported size should match the file size.
	expectedSize := int64(len(content))
	if size != expectedSize {
		t.Errorf("Expected deleted size %d, got %d", expectedSize, size)
	}

	// The file should be gone.
	if _, err := os.Stat(testFile); !os.IsNotExist(err) {
		t.Error("Expected file to be deleted")
	}
}

// TestJavaJarsPlugin_Create_LocalFile tests Create with a local file.
func TestJavaJarsPlugin_Create_LocalFile(t *testing.T) {
	tmpDir := t.TempDir()
	jarFile := filepath.Join(tmpDir, "test.jar")
	content := []byte("fake jar content")
	if err := os.WriteFile(jarFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test JAR file: %v", err)
	}

	plugin := &JavaJarsPlugin{
		resourcesDir: tmpDir,
	}

	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{}
	context := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, jarFile, runtimeEnv, context)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expectedSize := int64(len(content))
	if size != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, size)
	}
}
