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
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/common"
)

// ============ _mibString tests ============

func TestMibString(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{
			name:     "zero_bytes",
			input:    0,
			expected: "0.00MiB",
		},
		{
			name:     "one_mib",
			input:    1024 * 1024,
			expected: "1.00MiB",
		},
		{
			name:     "half_mib",
			input:    512 * 1024,
			expected: "0.50MiB",
		},
		{
			name:     "two_and_half_mib",
			input:    2.5 * 1024 * 1024,
			expected: "2.50MiB",
		},
		{
			name:     "ten_mib",
			input:    10 * 1024 * 1024,
			expected: "10.00MiB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := _mibString(tt.input)
			if result != tt.expected {
				t.Errorf("_mibString(%d) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// ============ _toExtendedLengthPath tests ============

func TestToExtendedLengthPath_Posix(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Skipping POSIX test on non-POSIX system")
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "relative_path",
			input:    "test/path",
			expected: "test/path",
		},
		{
			name:     "absolute_path",
			input:    "/tmp/test",
			expected: "/tmp/test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := _toExtendedLengthPath(tt.input)
			// On POSIX the path should be unchanged (or only cleaned).
			if !strings.HasSuffix(result, tt.expected) {
				t.Errorf("_toExtendedLengthPath(%q) = %q, expected to end with %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToExtendedLengthPath_Windows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Skipping Windows test on non-Windows")
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "already_extended",
			input:    `\\?\C:\path\to\file`,
			expected: `\\?\C:\path\to\file`,
		},
		{
			name:     "unc_path",
			input:    `\\server\share\file`,
			expected: `\\?\UNC\server\share\file`,
		},
		{
			name:     "local_path",
			input:    `C:\path\to\file`,
			expected: `\\?\C:\path\to\file`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := _toExtendedLengthPath(tt.input)
			if result != tt.expected {
				t.Errorf("_toExtendedLengthPath(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// ============ _xorBytes tests ============

func TestXorBytes(t *testing.T) {
	tests := []struct {
		name     string
		left     []byte
		right    []byte
		expected []byte
	}{
		{
			name:     "both_empty",
			left:     []byte{},
			right:    []byte{},
			expected: []byte{},
		},
		{
			name:     "left_empty",
			left:     []byte{},
			right:    []byte{1, 2, 3},
			expected: []byte{1, 2, 3},
		},
		{
			name:     "right_empty",
			left:     []byte{1, 2, 3},
			right:    []byte{},
			expected: []byte{1, 2, 3},
		},
		{
			name:     "same_length",
			left:     []byte{0x00, 0x00, 0x00, 0x00},
			right:    []byte{0xFF, 0xFF, 0xFF, 0xFF},
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:     "different_length_left_longer",
			left:     []byte{0x00, 0x00, 0x00, 0x00, 0x00},
			right:    []byte{0xFF, 0xFF},
			expected: []byte{0xFF, 0xFF},
		},
		{
			name:     "different_length_right_longer",
			left:     []byte{0x00, 0x00},
			right:    []byte{0xFF, 0xFF, 0xFF, 0xFF},
			expected: []byte{0xFF, 0xFF},
		},
		{
			name:     "xor_example",
			left:     []byte{0xAA, 0xBB, 0xCC},
			right:    []byte{0x11, 0x22, 0x33},
			expected: []byte{0xBB, 0x99, 0xFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := _xorBytes(tt.left, tt.right)
			if len(result) != len(tt.expected) {
				t.Fatalf("_xorBytes() returned result of length %d, expected %d", len(result), len(tt.expected))
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("_xorBytes()[%d] = 0x%02X, expected 0x%02X", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

// ============ _dirTravel tests ============

func TestDirTravel_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create the test directory structure.
	testFiles := []string{
		"file1.txt",
		"subdir/file2.txt",
		"subdir/nested/file3.txt",
	}

	for _, f := range testFiles {
		fullPath := filepath.Join(tmpDir, f)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	var visited []string

	handler := func(path string) error {
		visited = append(visited, path)
		return nil
	}

	err := _dirTravel(tmpDir, nil, handler, false)
	if err != nil {
		t.Errorf("_dirTravel() returned error: %v", err)
	}

	// Every file and directory should be visited.
	if len(visited) < len(testFiles) {
		t.Errorf("expected to visit at least %d paths, got %d", len(testFiles), len(visited))
	}
}

func TestDirTravel_WithExcludes(t *testing.T) {
	tmpDir := t.TempDir()

	// Create the test directory structure.
	testFiles := []string{
		"file1.txt",
		"node_modules/pkg/file.js",
		"src/main.go",
	}

	for _, f := range testFiles {
		fullPath := filepath.Join(tmpDir, f)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	var visited []string

	// Exclude node_modules.
	excludeFunc := func(p string) bool {
		return strings.Contains(p, "node_modules")
	}

	handler := func(path string) error {
		visited = append(visited, path)
		return nil
	}

	err := _dirTravel(tmpDir, []func(string) bool{excludeFunc}, handler, false)
	if err != nil {
		t.Errorf("_dirTravel() returned error: %v", err)
	}

	// Check that node_modules was not visited.
	for _, v := range visited {
		if strings.Contains(v, "node_modules") {
			t.Errorf("should not have visited node_modules path: %s", v)
		}
	}
}

func TestDirTravel_HandlerError(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	handlerErr := fmt.Errorf("handler error")

	handler := func(path string) error {
		return handlerErr
	}

	err := _dirTravel(tmpDir, nil, handler, false)
	if err != handlerErr {
		t.Errorf("expected handler error %v, got %v", handlerErr, err)
	}
}

// ============ _hashFileContentOrDirectoryName tests ============

func TestHashFileContentOrDirectoryName_File(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := "test content"
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	hash, err := _hashFileContentOrDirectoryName(testFile, tmpDir)
	if err != nil {
		t.Errorf("_hashFileContentOrDirectoryName() returned error: %v", err)
	}
	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}
}

func TestHashFileContentOrDirectoryName_Directory(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	hash, err := _hashFileContentOrDirectoryName(subDir, tmpDir)
	if err != nil {
		t.Errorf("_hashFileContentOrDirectoryName() returned error: %v", err)
	}
	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}
}

func TestHashFileContentOrDirectoryName_NonExistent(t *testing.T) {
	_, err := _hashFileContentOrDirectoryName("/nonexistent/path", "/tmp")
	if err == nil {
		t.Error("expected error for nonexistent path")
	}
}

// ============ _hashFile tests ============

func TestHashFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := "test content"
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	hash, err := _hashFile(testFile, tmpDir)
	if err != nil {
		t.Errorf("_hashFile() returned error: %v", err)
	}
	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}
}

// ============ _hashDirectory tests ============

func TestHashDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files.
	testFiles := []string{
		"file1.txt",
		"subdir/file2.txt",
	}
	for _, f := range testFiles {
		fullPath := filepath.Join(tmpDir, f)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	hash, err := _hashDirectory(tmpDir, tmpDir, nil, false)
	if err != nil {
		t.Errorf("_hashDirectory() returned error: %v", err)
	}
	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}
}

// ============ parsePath tests ============

func TestParsePath_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	err := parsePath(tmpDir)
	if err != nil {
		t.Errorf("parsePath(%q) returned unexpected error: %v", tmpDir, err)
	}
}

func TestParsePath_Invalid(t *testing.T) {
	err := parsePath("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("expected error for invalid path")
	}
	if !strings.Contains(err.Error(), "is not a valid path") {
		t.Errorf("expected 'is not a valid path' error, got %v", err)
	}
}

// ============ ParseURI tests ============

func TestParseURI_PathInsteadOfURI(t *testing.T) {
	// Skip on Windows.
	if runtime.GOOS == "windows" {
		t.Skip("Skipping path test on Windows")
	}

	_, _, err := ParseURI("/tmp/test.zip")
	if err == nil {
		t.Error("expected error when passing path instead of URI")
	}
	if !strings.Contains(err.Error(), "expected URI but received path") {
		t.Errorf("expected 'expected URI but received path' error, got %v", err)
	}
}

func TestParseURI_InvalidURI(t *testing.T) {
	_, _, err := ParseURI("://invalid-uri")
	if err == nil {
		t.Error("expected error for invalid URI")
	}
}

func TestParseURI_UnsupportedProtocol(t *testing.T) {
	_, _, err := ParseURI("unsupported://example.com/file.zip")
	if err == nil {
		t.Error("expected error for unsupported protocol")
	}
	if !strings.Contains(err.Error(), "invalid protocol") {
		t.Errorf("expected 'invalid protocol' error, got %v", err)
	}
}

func TestParseURI_HttpsWheel(t *testing.T) {
	protocol, packageName, err := ParseURI("https://example.com/package.whl")
	if err != nil {
		t.Errorf("ParseURI() returned error: %v", err)
	}
	if protocol != ProtocolHTTPS {
		t.Errorf("expected protocol %s, got %s", ProtocolHTTPS, protocol)
	}
	if packageName != "package.whl" {
		t.Errorf("expected package name 'package.whl', got %s", packageName)
	}
}

func TestParseURI_HttpsZip(t *testing.T) {
	protocol, packageName, err := ParseURI("https://example.com/path/to/package.zip")
	if err != nil {
		t.Errorf("ParseURI() returned error: %v", err)
	}
	if protocol != ProtocolHTTPS {
		t.Errorf("expected protocol %s, got %s", ProtocolHTTPS, protocol)
	}
	// Verify the package name is sanitized correctly.
	if !strings.HasSuffix(packageName, ".zip") {
		t.Errorf("expected package name to end with .zip, got %s", packageName)
	}
}

func TestParseURI_File(t *testing.T) {
	protocol, packageName, err := ParseURI("file:///tmp/package.zip")
	if err != nil {
		t.Errorf("ParseURI() returned error: %v", err)
	}
	if protocol != ProtocolFile {
		t.Errorf("expected protocol %s, got %s", ProtocolFile, protocol)
	}
	// The file:// protocol counts as remote, so the package name keeps the protocol prefix and path.
	expectedName := "file__tmp_package.zip"
	if packageName != expectedName {
		t.Errorf("expected package name %q for file protocol, got %q", expectedName, packageName)
	}
}

// ============ IsZipURI tests ============

func TestIsZipURI_True(t *testing.T) {
	tests := []string{
		"https://example.com/package.zip",
		"https://s3.example.com/bucket/file.zip",
		"https://gs.example.com/bucket/archive.zip",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if !IsZipURI(uri) {
				t.Errorf("IsZipURI(%q) should return true", uri)
			}
		})
	}
}

func TestIsZipURI_False(t *testing.T) {
	tests := []string{
		"https://example.com/package.whl",
		"https://s3.example.com/bucket/file.jar",
		"invalid-uri",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if IsZipURI(uri) {
				t.Errorf("IsZipURI(%q) should return false", uri)
			}
		})
	}
}

// ============ IsWhlURI tests ============

func TestIsWhlURI_True(t *testing.T) {
	tests := []string{
		"https://example.com/package.whl",
		"https://s3.example.com/bucket/file.whl",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if !IsWhlURI(uri) {
				t.Errorf("IsWhlURI(%q) should return true", uri)
			}
		})
	}
}

func TestIsWhlURI_False(t *testing.T) {
	tests := []string{
		"https://example.com/package.zip",
		"https://s3.example.com/bucket/file.jar",
		"invalid-uri",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if IsWhlURI(uri) {
				t.Errorf("IsWhlURI(%q) should return false", uri)
			}
		})
	}
}

// ============ IsJarURI tests ============

func TestIsJarURI_True(t *testing.T) {
	tests := []string{
		"https://example.com/package.jar",
		"https://s3.example.com/bucket/file.jar",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if !IsJarURI(uri) {
				t.Errorf("IsJarURI(%q) should return true", uri)
			}
		})
	}
}

func TestIsJarURI_False(t *testing.T) {
	tests := []string{
		"https://example.com/package.zip",
		"https://s3.example.com/bucket/file.whl",
		"invalid-uri",
	}

	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			if IsJarURI(uri) {
				t.Errorf("IsJarURI(%q) should return false", uri)
			}
		})
	}
}

// ============ _getExcludes tests ============

func TestGetExcludes(t *testing.T) {
	excludes := []string{"*.log", "node_modules/", "__pycache__/"}
	excludeFunc := _getExcludes("/tmp", excludes)

	tests := []struct {
		path     string
		expected bool
	}{
		{"/tmp/test.log", true},
		{"/tmp/node_modules/pkg/index.js", true},
		{"/tmp/__pycache__/module.pyc", true},
		{"/tmp/src/main.go", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := excludeFunc(tt.path)
			if result != tt.expected {
				t.Errorf("_getExcludes()(path=%q) = %v, expected %v", tt.path, result, tt.expected)
			}
		})
	}
}

// ============ _getIgnoreFile tests ============

func TestGetIgnoreFile_Existing(t *testing.T) {
	tmpDir := t.TempDir()
	ignoreFilePath := filepath.Join(tmpDir, ".gitignore")
	if err := os.WriteFile(ignoreFilePath, []byte("*.log\nnode_modules/\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ignoreFunc, err := _getIgnoreFile(tmpDir, ".gitignore")
	if err != nil {
		t.Errorf("_getIgnoreFile() returned error: %v", err)
	}
	if ignoreFunc == nil {
		t.Error("expected non-nil ignore function")
	}

	// Test the ignore rules.
	if !ignoreFunc(filepath.Join(tmpDir, "test.log")) {
		t.Error("expected *.log to be ignored")
	}
	if ignoreFunc(filepath.Join(tmpDir, "main.go")) {
		t.Error("expected main.go to not be ignored")
	}
}

func TestGetIgnoreFile_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	ignoreFunc, err := _getIgnoreFile(tmpDir, ".gitignore")
	if err != nil {
		t.Errorf("_getIgnoreFile() returned unexpected error: %v", err)
	}
	if ignoreFunc != nil {
		t.Error("expected nil ignore function for non-existent file")
	}
}

// ============ getExcludesFromIgnoreFiles tests ============

func TestGetExcludesFromIgnoreFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .gitignore file.
	gitignorePath := filepath.Join(tmpDir, ".gitignore")
	if err := os.WriteFile(gitignorePath, []byte("*.log\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a .rayignore file.
	rayignorePath := filepath.Join(tmpDir, ".rayignore")
	if err := os.WriteFile(rayignorePath, []byte("*.tmp\n"), 0644); err != nil {
		t.Fatal(err)
	}

	excludes, err := getExcludesFromIgnoreFiles(tmpDir, true)
	if err != nil {
		t.Errorf("getExcludesFromIgnoreFiles() returned error: %v", err)
	}
	if len(excludes) != 2 {
		t.Errorf("expected 2 exclude functions, got %d", len(excludes))
	}
}

func TestGetExcludesFromIgnoreFiles_OnlyRayignore(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .rayignore file.
	rayignorePath := filepath.Join(tmpDir, ".rayignore")
	if err := os.WriteFile(rayignorePath, []byte("*.tmp\n"), 0644); err != nil {
		t.Fatal(err)
	}

	excludes, err := getExcludesFromIgnoreFiles(tmpDir, true)
	if err != nil {
		t.Errorf("getExcludesFromIgnoreFiles() returned error: %v", err)
	}
	if len(excludes) != 1 {
		t.Errorf("expected 1 exclude function, got %d", len(excludes))
	}
}

func TestGetExcludesFromIgnoreFiles_NoFiles(t *testing.T) {
	tmpDir := t.TempDir()

	excludes, err := getExcludesFromIgnoreFiles(tmpDir, false)
	if err != nil {
		t.Errorf("getExcludesFromIgnoreFiles() returned error: %v", err)
	}
	// With no .gitignore or .rayignore files the result should be empty.
	if len(excludes) != 0 {
		t.Errorf("expected 0 exclude functions, got %d", len(excludes))
	}
}

// ============ _getLocalPath tests ============

func TestGetLocalPath(t *testing.T) {
	baseDir := "/tmp"
	uri := "https://example.com/package.zip"

	result := _getLocalPath(baseDir, uri)
	// A remote URI's package name keeps the protocol and host prefix.
	expected := filepath.Join(baseDir, "https_example_com_package.zip")

	if result != expected {
		t.Errorf("_getLocalPath(%q, %q) = %q, expected %q", baseDir, uri, result, expected)
	}
}

// ============ GetLocalDirFromURI tests ============

func TestGetLocalDirFromURI_Zip(t *testing.T) {
	baseDir := "/tmp"
	uri := "https://example.com/package.zip"

	result := GetLocalDirFromURI(uri, baseDir)
	// A remote URI's directory name keeps the protocol and host prefix.
	expected := filepath.Join(baseDir, "https_example_com_package")

	if result != expected {
		t.Errorf("GetLocalDirFromURI(%q, %q) = %q, expected %q", uri, baseDir, result, expected)
	}
}

func TestGetLocalDirFromURI_Whl(t *testing.T) {
	baseDir := "/tmp"
	uri := "https://example.com/package.whl"

	result := GetLocalDirFromURI(uri, baseDir)
	// .whl files get the extension stripped.
	expected := filepath.Join(baseDir, "package")

	if result != expected {
		t.Errorf("GetLocalDirFromURI(%q, %q) = %q, expected %q", uri, baseDir, result, expected)
	}
}

func TestGetLocalDirFromURI_Jar(t *testing.T) {
	baseDir := "/tmp"
	uri := "https://example.com/package.jar"

	result := GetLocalDirFromURI(uri, baseDir)
	// .jar files get the extension stripped.
	expected := filepath.Join(baseDir, "https_example_com_package")

	if result != expected {
		t.Errorf("GetLocalDirFromURI(%q, %q) = %q, expected %q", uri, baseDir, result, expected)
	}
}

// ============ GetTopLevelDirFromCompressedPackage tests ============

func TestGetTopLevelDirFromCompressedPackage_ValidZip(t *testing.T) {
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "test.zip")

	// Create a valid zip file (with a top-level directory).
	createTestZip(t, zipPath, "top_level_dir/file.txt")

	topLevelDir, err := GetTopLevelDirFromCompressedPackage(zipPath)
	if err != nil {
		t.Errorf("GetTopLevelDirFromCompressedPackage() returned error: %v", err)
	}
	if topLevelDir != "top_level_dir" {
		t.Errorf("expected top level directory 'top_level_dir', got %q", topLevelDir)
	}
}

func TestGetTopLevelDirFromCompressedPackage_NoTopLevelDir(t *testing.T) {
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "test.zip")

	// Create a zip file without a top-level directory (files at the root).
	createTestZipFlat(t, zipPath, "file1.txt", "file2.txt")

	topLevelDir, err := GetTopLevelDirFromCompressedPackage(zipPath)
	if err != nil {
		t.Errorf("GetTopLevelDirFromCompressedPackage() returned error: %v", err)
	}
	if topLevelDir != "" {
		t.Errorf("expected empty top level directory, got %q", topLevelDir)
	}
}

func TestGetTopLevelDirFromCompressedPackage_NonExistent(t *testing.T) {
	_, err := GetTopLevelDirFromCompressedPackage("/nonexistent.zip")
	if err == nil {
		t.Error("expected error for non-existent zip file")
	}
}

// Helper: create a test zip file (with a top-level directory).
func createTestZip(t *testing.T, zipPath, filePath string) {
	t.Helper()

	dir := filepath.Dir(filePath)
	fileName := filepath.Base(filePath)

	tmpDir := t.TempDir()
	fullPath := filepath.Join(tmpDir, filePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create the zip file with the zip command.
	cmd := fmt.Sprintf("cd %s && zip -r %s %s", tmpDir, zipPath, dir)
	if runtime.GOOS == "windows" {
		// Use PowerShell on Windows.
		cmd = fmt.Sprintf("cd %s; Compress-Archive -Path %s -DestinationPath %s -Force", tmpDir, dir, zipPath)
	}
	_ = cmd // Keep the test simple; do not actually run the command.
	// The zip command may be unavailable, so build a simple zip file instead.
	createSimpleZipWithDir(t, zipPath, dir, fileName)
}

// Helper: create a simple zip file (with a top-level directory).
func createSimpleZipWithDir(t *testing.T, zipPath, dirName, fileName string) {
	t.Helper()

	// Create the zip with the archive/zip package.
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zipFile.Close()

	w := newZipWriter(zipFile)
	defer w.Close()

	// Add the directory entry.
	_, err = w.Create(dirName + "/")
	if err != nil {
		t.Fatal(err)
	}

	// Add the file entry.
	header, err := zip.FileInfoHeader(&fakeFileInfo{name: fileName})
	if err != nil {
		t.Fatal(err)
	}
	header.Name = dirName + "/" + fileName
	header.Method = zip.Store

	fw, err := w.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("test content")); err != nil {
		t.Fatal(err)
	}
}

// Helper: create a flat zip file.
func createTestZipFlat(t *testing.T, zipPath string, files ...string) {
	t.Helper()

	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zipFile.Close()

	w := newZipWriter(zipFile)
	defer w.Close()

	for _, fileName := range files {
		header, err := zip.FileInfoHeader(&fakeFileInfo{name: fileName})
		if err != nil {
			t.Fatal(err)
		}
		header.Method = zip.Store

		fw, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write([]byte("test content")); err != nil {
			t.Fatal(err)
		}
	}
}

func newZipWriter(w io.Writer) *zip.Writer {
	return zip.NewWriter(w)
}

// fakeFileInfo is a fake FileInfo implementation for tests.
type fakeFileInfo struct {
	name string
}

func (f *fakeFileInfo) Name() string       { return f.name }
func (f *fakeFileInfo) Size() int64        { return 12 }
func (f *fakeFileInfo) Mode() os.FileMode  { return 0644 }
func (f *fakeFileInfo) ModTime() time.Time { return time.Now() }
func (f *fakeFileInfo) IsDir() bool        { return false }
func (f *fakeFileInfo) Sys() interface{}   { return nil }

// ============ removeDirFromFilepaths tests ============

func TestRemoveDirFromFilepaths(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")
	rdir := "test_dir"

	if err := os.MkdirAll(filepath.Join(baseDir, rdir), 0755); err != nil {
		t.Fatal(err)
	}

	// Create files in the directory.
	testFile := filepath.Join(baseDir, rdir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	err := removeDirFromFilepaths(baseDir, rdir)
	if err != nil {
		t.Errorf("removeDirFromFilepaths() returned error: %v", err)
	}

	// Verify the files were moved into baseDir.
	movedFile := filepath.Join(baseDir, "test.txt")
	if _, err := os.Stat(movedFile); os.IsNotExist(err) {
		t.Error("expected file to be moved to base directory")
	}
}

// ============ common.CopyAll tests ============

func TestCopyAll_File(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src.txt")
	dst := filepath.Join(tmpDir, "dst.txt")

	if err := os.WriteFile(src, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	err := common.CopyAll(src, dst)
	if err != nil {
		t.Errorf("common.CopyAll() returned error: %v", err)
	}

	content, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "test content" {
		t.Errorf("expected 'test content', got %q", string(content))
	}
}

func TestCopyAll_Directory(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src")
	dst := filepath.Join(tmpDir, "dst")

	if err := os.MkdirAll(src, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	err := common.CopyAll(src, dst)
	if err != nil {
		t.Errorf("common.CopyAll() returned error: %v", err)
	}

	if _, err := os.Stat(dst); os.IsNotExist(err) {
		t.Error("expected destination directory to exist")
	}
}

// ============ common.CopyFile tests ============

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src.txt")
	dst := filepath.Join(tmpDir, "dst.txt")

	if err := os.WriteFile(src, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	err := common.CopyFile(src, dst)
	if err != nil {
		t.Errorf("common.CopyFile() returned error: %v", err)
	}

	content, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "test content" {
		t.Errorf("expected 'test content', got %q", string(content))
	}
}

func TestCopyFile_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "nonexistent.txt")
	dst := filepath.Join(tmpDir, "dst.txt")

	err := common.CopyFile(src, dst)
	if err == nil {
		t.Error("expected error for non-existent source file")
	}
}

// ============ common.CopyDir tests ============

func TestCopyDir(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src")
	dst := filepath.Join(tmpDir, "dst")

	if err := os.MkdirAll(src, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	err := common.CopyDir(src, dst)
	if err != nil {
		t.Errorf("common.CopyDir() returned error: %v", err)
	}

	// Verify the files were copied.
	copiedFile := filepath.Join(dst, "file.txt")
	if _, err := os.Stat(copiedFile); os.IsNotExist(err) {
		t.Error("expected copied file to exist")
	}
}
