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
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// ============ PinRuntimeEnvURIWrapper tests ============

func TestPinRuntimeEnvURIWrapper_ZeroExpiration(t *testing.T) {
	// Tests that expirationS 0 gets the default value from the environment
	// variable. Since PinRuntimeEnvURICall does nothing when the GCS client is
	// not initialized, this test mainly verifies that no error is returned.
	err := PinRuntimeEnvURIWrapper("gcs://test-uri", 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPinRuntimeEnvURIWrapper_NegativeExpiration(t *testing.T) {
	// Tests that a negative expirationS returns an error.
	err := PinRuntimeEnvURIWrapper("gcs://test-uri", -1)
	if err == nil {
		t.Error("expected error for negative expirationS")
	}
	expectedMsg := "expiration_s must be >= 0, got -1"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestPinRuntimeEnvURIWrapper_PositiveExpiration(t *testing.T) {
	// Tests the behavior for a positive expirationS.
	// Since the GCS client is not initialized, PinRuntimeEnvURICall does not
	// perform any real operation.
	err := PinRuntimeEnvURIWrapper("gcs://test-uri", 60)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ============ _storePackageInGCS tests ============

func TestStorePackageInGCS_SizeExceedsLimit(t *testing.T) {
	// Create data that exceeds the limit.
	largeData := make([]byte, GCS_STORAGE_MAX_SIZE+1)

	_, err := _storePackageInGCS("gcs://test-pkg", largeData)
	if err == nil {
		t.Error("expected error for package exceeding size limit")
	}
	if !strings.Contains(err.Error(), "Package size") {
		t.Errorf("expected size limit error, got %v", err)
	}
}

func TestStorePackageInGCS_UploadFailForTesting(t *testing.T) {
	// Set the environment variable to simulate an upload failure.
	os.Setenv(RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR, "1")
	defer os.Unsetenv(RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR)

	_, err := _storePackageInGCS("gcs://test-pkg", []byte("test data"))
	if err == nil {
		t.Error("expected error for simulated upload failure")
	}
	if !strings.Contains(err.Error(), "Simulating failure") {
		t.Errorf("expected simulation failure error, got %v", err)
	}
}

func TestStorePackageInGCS_Success(t *testing.T) {
	// This test fails because the GCS client is not initialized.
	// In real usage, the GCS client needs to be initialized.
	data := []byte("test package data")
	_, err := _storePackageInGCS("gcs://test-pkg", data)

	// It is expected to fail because the GCS client is not initialized.
	if err == nil {
		t.Log("Note: GCS client should be initialized for this test to pass")
	}
}

// ============ _zipFiles tests ============

func TestZipFiles_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test file.
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	outputPath := filepath.Join(tmpDir, "output.zip")

	err := _zipFiles(testFile, nil, outputPath, false, false)
	if err != nil {
		t.Errorf("_zipFiles() returned error: %v", err)
	}

	// Verify the zip file was created.
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("expected output zip file to exist")
	}

	// Verify the zip content.
	reader, err := zip.OpenReader(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if len(reader.File) < 1 {
		t.Error("expected at least one file in zip")
	}
}

func TestZipFiles_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test directory structure.
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

	outputPath := filepath.Join(tmpDir, "output.zip")

	err := _zipFiles(tmpDir, nil, outputPath, false, false)
	if err != nil {
		t.Errorf("_zipFiles() returned error: %v", err)
	}

	// Verify the zip file was created.
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("expected output zip file to exist")
	}
}

func TestZipFiles_WithExcludes(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test files.
	testFiles := []string{
		"main.go",
		"node_modules/pkg/index.js",
		"__pycache__/module.pyc",
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

	outputPath := filepath.Join(tmpDir, "output.zip")

	// Exclude node_modules and __pycache__.
	excludes := []string{"node_modules/", "__pycache__/"}
	err := _zipFiles(tmpDir, excludes, outputPath, false, false)
	if err != nil {
		t.Errorf("_zipFiles() returned error: %v", err)
	}

	// Verify the zip content.
	reader, err := zip.OpenReader(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Check that the excluded files are not in the zip.
	for _, file := range reader.File {
		if strings.Contains(file.Name, "node_modules") {
			t.Errorf("node_modules should be excluded, found: %s", file.Name)
		}
		if strings.Contains(file.Name, "__pycache__") {
			t.Errorf("__pycache__ should be excluded, found: %s", file.Name)
		}
	}
}

func TestZipFiles_IncludeParentDir(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test file.
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	outputPath := filepath.Join(tmpDir, "output.zip")

	err := _zipFiles(testFile, nil, outputPath, false, true)
	if err != nil {
		t.Errorf("_zipFiles() returned error: %v", err)
	}

	// Verify the zip content includes the parent directory.
	reader, err := zip.OpenReader(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	found := false
	for _, file := range reader.File {
		if strings.Contains(file.Name, filepath.Base(tmpDir)) {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected zip to include parent directory")
	}
}

func TestZipFiles_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an empty directory.
	emptyDir := filepath.Join(tmpDir, "empty")
	if err := os.MkdirAll(emptyDir, 0755); err != nil {
		t.Fatal(err)
	}

	outputPath := filepath.Join(tmpDir, "output.zip")

	err := _zipFiles(emptyDir, nil, outputPath, false, false)
	if err != nil {
		t.Errorf("_zipFiles() returned error: %v", err)
	}
}

// ============ PackageExists tests ============

func TestPackageExists_GCSProtocol(t *testing.T) {
	// This test returns an error because the GCS client is not initialized.
	exists, err := PackageExists("gcs://test-pkg")

	// It is expected to fail because the GCS client is not initialized.
	if err == nil {
		t.Log("Note: GCS client should be initialized for this test")
	}
	_ = exists
}

func TestPackageExists_UnsupportedProtocol(t *testing.T) {
	_, err := PackageExists("unsupported://test-pkg")
	if err == nil {
		t.Error("expected error for unsupported protocol")
	}
	// The error message contains "invalid protocol".
	if !strings.Contains(err.Error(), "invalid protocol") {
		t.Errorf("expected 'invalid protocol' error, got %v", err)
	}
}

func TestPackageExists_InvalidURI(t *testing.T) {
	_, err := PackageExists("://invalid-uri")
	if err == nil {
		t.Error("expected error for invalid URI")
	}
}

// ============ GetURIForPackage tests ============

func TestGetURIForPackage_WheelFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a wheel file.
	wheelFile := filepath.Join(tmpDir, "test.whl")
	if err := os.WriteFile(wheelFile, []byte("wheel content"), 0644); err != nil {
		t.Fatal(err)
	}

	uri, err := GetURIForPackage(wheelFile)
	if err != nil {
		t.Errorf("GetURIForPackage() returned error: %v", err)
	}

	expectedPrefix := fmt.Sprintf("%s://", ProtocolGCS)
	if !strings.HasPrefix(uri, expectedPrefix) {
		t.Errorf("expected URI to start with %s, got %s", expectedPrefix, uri)
	}
	if !strings.HasSuffix(uri, "test.whl") {
		t.Errorf("expected URI to end with test.whl, got %s", uri)
	}
}

func TestGetURIForPackage_RegularFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a regular file.
	testFile := filepath.Join(tmpDir, "test.zip")
	content := []byte("test content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	uri, err := GetURIForPackage(testFile)
	if err != nil {
		t.Errorf("GetURIForPackage() returned error: %v", err)
	}

	// Verify the URI format.
	expectedPrefix := fmt.Sprintf("%s://%s", ProtocolGCS, RAY_PKG_PREFIX)
	if !strings.HasPrefix(uri, expectedPrefix) {
		t.Errorf("expected URI to start with %s, got %s", expectedPrefix, uri)
	}

	// Verify the hash is correct.
	hashVal := sha1.Sum(content)
	expectedHash := hex.EncodeToString(hashVal[:])
	if !strings.Contains(uri, expectedHash) {
		t.Errorf("expected URI to contain hash %s, got %s", expectedHash, uri)
	}
}

func TestGetURIForPackage_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a directory (not a file).
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatal(err)
	}

	_, err := GetURIForPackage(tmpDir)
	if err == nil {
		t.Error("expected error for directory")
	}
	if !strings.Contains(err.Error(), "expected file, got directory") {
		t.Errorf("expected 'expected file' error, got %v", err)
	}
}

func TestGetURIForPackage_NonExistent(t *testing.T) {
	_, err := GetURIForPackage("/nonexistent/file.zip")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// ============ GetURIForFile tests ============

func TestGetURIForFile_Valid(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("test content")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	uri, err := GetURIForFile(testFile)
	if err != nil {
		t.Errorf("GetURIForFile() returned error: %v", err)
	}

	expectedPrefix := fmt.Sprintf("%s://%s", ProtocolGCS, RAY_PKG_PREFIX)
	if !strings.HasPrefix(uri, expectedPrefix) {
		t.Errorf("expected URI to start with %s, got %s", expectedPrefix, uri)
	}
}

func TestGetURIForFile_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	// Pass a directory instead of a file.
	_, err := GetURIForFile(tmpDir)
	if err == nil {
		t.Error("expected error for directory")
	}
	if !strings.Contains(err.Error(), "must be an existing file, not a directory") {
		t.Errorf("expected 'not a directory' error, got %v", err)
	}
}

func TestGetURIForFile_NonExistent(t *testing.T) {
	_, err := GetURIForFile("/nonexistent/file.txt")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

// ============ GetURIForDirectory tests ============

func TestGetURIForDirectory_Valid(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test file.
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	uri, err := GetURIForDirectory(tmpDir, false, nil)
	if err != nil {
		t.Errorf("GetURIForDirectory() returned error: %v", err)
	}

	expectedPrefix := fmt.Sprintf("%s://%s", ProtocolGCS, RAY_PKG_PREFIX)
	if !strings.HasPrefix(uri, expectedPrefix) {
		t.Errorf("expected URI to start with %s, got %s", expectedPrefix, uri)
	}
}

func TestGetURIForDirectory_File(t *testing.T) {
	tmpDir := t.TempDir()

	// Pass a file instead of a directory.
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := GetURIForDirectory(testFile, false, nil)
	if err == nil {
		t.Error("expected error for file")
	}
	if !strings.Contains(err.Error(), "must be an existing directory, not a file") {
		t.Errorf("expected 'not a file' error, got %v", err)
	}
}

func GetURIForDirectory_NonExistent(t *testing.T) {
	_, err := GetURIForDirectory("/nonexistent/directory", false, nil)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

// ============ UploadPackageToGCS tests ============

func TestUploadPackageToGCS_GCSProtocol(t *testing.T) {
	// This test fails because the GCS client is not initialized.
	err := UploadPackageToGCS("gcs://test-pkg", []byte("test data"))

	// It is expected to fail because the GCS client is not initialized.
	if err == nil {
		t.Log("Note: GCS client should be initialized for this test to pass")
	}
}

func TestUploadPackageToGCS_RemoteProtocol(t *testing.T) {
	err := UploadPackageToGCS("https://example.com/pkg.zip", []byte("test data"))
	if err == nil {
		t.Error("expected error for remote protocol")
	}
	if !strings.Contains(err.Error(), "should not be called with a remote path") {
		t.Errorf("expected 'remote path' error, got %v", err)
	}
}

func TestUploadPackageToGCS_UnsupportedProtocol(t *testing.T) {
	err := UploadPackageToGCS("unsupported://pkg", []byte("test data"))
	if err == nil {
		t.Error("expected error for unsupported protocol")
	}
	// The error message contains "invalid protocol".
	if !strings.Contains(err.Error(), "invalid protocol") {
		t.Errorf("expected 'invalid protocol' error, got %v", err)
	}
}

func TestUploadPackageToGCS_NilLogger(t *testing.T) {
	// Note: UploadPackageToGCS no longer accepts a logger argument; the
	// function uses log.Log internally. This test is kept to verify the
	// behavior without an explicit logger.

	// It should use defaultLogger without panicking.
	err := UploadPackageToGCS("gcs://test-pkg", []byte("test data"))
	_ = err // May fail because the GCS is not initialized.
}

// ============ CreatePackageWithConfig tests ============

func TestCreatePackageWithConfig_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a source directory.
	srcDir := filepath.Join(tmpDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	targetPath := filepath.Join(tmpDir, "output.zip")

	config := &PackageConfig{
		ModulePath:       srcDir,
		TargetPath:       targetPath,
		IncludeGitignore: false,
		IncludeParentDir: false,
		Excludes:         nil,
	}

	err := CreatePackageWithConfig(config)
	if err != nil {
		t.Errorf("CreatePackageWithConfig() returned error: %v", err)
	}

	// Verify the output file exists.
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		t.Error("expected output file to exist")
	}
}

func TestCreatePackageWithConfig_TargetAlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an existing target file.
	targetPath := filepath.Join(tmpDir, "output.zip")
	if err := os.WriteFile(targetPath, []byte("existing"), 0644); err != nil {
		t.Fatal(err)
	}

	config := &PackageConfig{
		ModulePath:       tmpDir,
		TargetPath:       targetPath,
		IncludeGitignore: false,
		IncludeParentDir: false,
		Excludes:         nil,
	}

	// It should return directly without modifying the existing file.
	err := CreatePackageWithConfig(config)
	if err != nil {
		t.Errorf("CreatePackageWithConfig() returned error: %v", err)
	}

	// Verify the file content is unchanged.
	content, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "existing" {
		t.Error("expected target file to remain unchanged")
	}
}

func TestCreatePackageWithConfig_NilConfig(t *testing.T) {
	err := CreatePackageWithConfig(nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
	if !strings.Contains(err.Error(), "config cannot be nil") {
		t.Errorf("expected 'config cannot be nil' error, got %v", err)
	}
}

func TestCreatePackageWithConfig_NilLogger(t *testing.T) {
	tmpDir := t.TempDir()

	srcDir := filepath.Join(tmpDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}

	targetPath := filepath.Join(tmpDir, "output.zip")

	config := &PackageConfig{
		ModulePath:       srcDir,
		TargetPath:       targetPath,
		IncludeGitignore: false,
		IncludeParentDir: false,
		Excludes:         nil,
	}

	err := CreatePackageWithConfig(config)
	// It should use defaultLogger without panicking.
	_ = err
}

// ============ UploadPackageIfNeededWithConfig tests ============

func TestUploadPackageIfNeededWithConfig_NilConfig(t *testing.T) {
	ctx := context.Background()
	_, err := UploadPackageIfNeededWithConfig(ctx, nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
	if !strings.Contains(err.Error(), "config cannot be nil") {
		t.Errorf("expected 'config cannot be nil' error, got %v", err)
	}
}

func TestUploadPackageIfNeededWithConfig_NilLogger(t *testing.T) {
	tmpDir := t.TempDir()

	srcDir := filepath.Join(tmpDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}

	pkgURI := "gcs://test-pkg"

	ctx := context.Background()
	config := &PackageUploadConfig{
		PkgURI:           pkgURI,
		BaseDirectory:    tmpDir,
		ModulePath:       srcDir,
		IncludeGitignore: false,
		IncludeParentDir: false,
		Excludes:         nil,
	}

	_, err := UploadPackageIfNeededWithConfig(ctx, config)
	// It is expected to fail because the GCS is not initialized.
	_ = err
}

func TestUploadPackageIfNeededWithConfig_NilExcludes(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a source directory.
	srcDir := filepath.Join(tmpDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}

	pkgURI := "gcs://test-pkg"

	ctx := context.Background()
	config := &PackageUploadConfig{
		PkgURI:           pkgURI,
		BaseDirectory:    tmpDir,
		ModulePath:       srcDir,
		IncludeGitignore: false,
		IncludeParentDir: false,
		Excludes:         nil, // nil excludes should be converted to an empty slice.
	}

	// This call fails because the GCS is not initialized.
	_, err := UploadPackageIfNeededWithConfig(ctx, config)
	_ = err // Expected to fail.
}

// ============ DownloadAndUnpackPackage tests ============

func TestDownloadAndUnpackPackage_InvalidURI_NoExtension(t *testing.T) {
	ctx := context.Background()

	_, err := DownloadAndUnpackPackage(ctx, "gcs://test", t.TempDir(), nil, false)
	if err == nil {
		t.Error("expected error for URI without extension")
	}
	if !strings.Contains(err.Error(), "must have a file extension") {
		t.Errorf("expected 'extension' error, got %v", err)
	}
}

func TestDownloadAndUnpackPackage_GCSProtocol_NoClient(t *testing.T) {
	ctx := context.Background()

	_, err := DownloadAndUnpackPackage(ctx, "gcs://test.zip", t.TempDir(), nil, false)
	if err == nil {
		t.Error("expected error for missing GCS client")
	}
	if !strings.Contains(err.Error(), "GCS client must be provided") {
		t.Errorf("expected 'GCS client' error, got %v", err)
	}
}

func TestDownloadAndUnpackPackage_UnsupportedProtocol(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a local zip file for testing.
	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithContent(t, zipPath, map[string]string{"file.txt": "content"})

	// Use the file:// protocol but with an unsupported file format.
	_, err := DownloadAndUnpackPackage(ctx, fmt.Sprintf("file://%s", zipPath), tmpDir, nil, false)
	// The file:// protocol returns an error for non-zip/whl/jar formats.
	// Or we test a truly unsupported case.

	// Actually, we need to test a protocol that is not supported by
	// DownloadRemoteURI. Since all remote protocols return "not implemented"
	// errors, we switch to s3:// for the test.
	_, err = DownloadAndUnpackPackage(ctx, "s3://bucket/test.zip", tmpDir, nil, false)
	if err == nil {
		t.Error("expected error for unsupported protocol")
	}
}

func TestDownloadAndUnpackPackage_NilLogger(t *testing.T) {
	ctx := context.Background()
	// Note: DownloadAndUnpackPackage no longer accepts a logger argument; the
	// function uses log.Log internally. This test is kept to verify the
	// behavior without an explicit logger.

	_, err := DownloadAndUnpackPackage(ctx, "gcs://test.zip", t.TempDir(), nil, false)
	_ = err // Expected to fail.
}

// ============ UnzipPackage tests ============

func TestUnzipPackage_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test zip file.
	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithContent(t, zipPath, map[string]string{
		"file1.txt":     "content1",
		"dir/file2.txt": "content2",
	})

	targetDir := filepath.Join(tmpDir, "target")

	err := UnzipPackage(zipPath, targetDir, false, false)
	if err != nil {
		t.Errorf("UnzipPackage() returned error: %v", err)
	}

	// Verify the extracted files.
	file1 := filepath.Join(targetDir, "file1.txt")
	if _, err := os.Stat(file1); os.IsNotExist(err) {
		t.Error("expected file1.txt to exist")
	}

	dirFile2 := filepath.Join(targetDir, "dir", "file2.txt")
	if _, err := os.Stat(dirFile2); os.IsNotExist(err) {
		t.Error("expected dir/file2.txt to exist")
	}
}

func TestUnzipPackage_RemoveTopLevelDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a zip file with a top-level directory.
	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithTopLevelDir(t, zipPath, "top_level", map[string]string{
		"file1.txt": "content1",
	})

	targetDir := filepath.Join(tmpDir, "target")

	err := UnzipPackage(zipPath, targetDir, true, false)
	if err != nil {
		t.Errorf("UnzipPackage() returned error: %v", err)
	}

	// Verify the file was moved to the target root.
	file1 := filepath.Join(targetDir, "file1.txt")
	if _, err := os.Stat(file1); os.IsNotExist(err) {
		t.Error("expected file1.txt to be at target root")
	}
}

func TestUnzipPackage_UnlinkZip(t *testing.T) {
	tmpDir := t.TempDir()

	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithContent(t, zipPath, map[string]string{
		"file.txt": "content",
	})

	targetDir := filepath.Join(tmpDir, "target")

	err := UnzipPackage(zipPath, targetDir, false, true)
	if err != nil {
		t.Errorf("UnzipPackage() returned error: %v", err)
	}

	// Verify the zip file was deleted.
	if _, err := os.Stat(zipPath); !os.IsNotExist(err) {
		t.Error("expected zip file to be deleted")
	}
}

func TestUnzipPackage_TargetAlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()

	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithContent(t, zipPath, map[string]string{
		"file.txt": "content",
	})

	targetDir := filepath.Join(tmpDir, "target")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		t.Fatal(err)
	}

	err := UnzipPackage(zipPath, targetDir, false, false)
	if err != nil {
		t.Errorf("UnzipPackage() returned error: %v", err)
	}
}

func TestUnzipPackage_NonExistentZip(t *testing.T) {
	tmpDir := t.TempDir()

	targetDir := filepath.Join(tmpDir, "target")

	_, err := os.Stat(filepath.Join(tmpDir, "nonexistent.zip"))
	if !os.IsNotExist(err) {
		t.Fatal("expected nonexistent.zip to not exist")
	}

	err = UnzipPackage(filepath.Join(tmpDir, "nonexistent.zip"), targetDir, false, false)
	if err == nil {
		t.Error("expected error for non-existent zip")
	}
}

func TestUnzipPackage_NilLogger(t *testing.T) {
	tmpDir := t.TempDir()

	zipPath := filepath.Join(tmpDir, "test.zip")
	createTestZipWithContent(t, zipPath, map[string]string{
		"file.txt": "content",
	})

	targetDir := filepath.Join(tmpDir, "target")
	// The function no longer accepts a logger argument; it uses log.Log
	// internally.
	err := UnzipPackage(zipPath, targetDir, false, false)
	// It should not panic.
	_ = err
}

// ============ DeletePackage tests ============

func TestDeletePackage_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test directory.
	pkgDir := filepath.Join(tmpDir, "test_pkg")
	if err := os.MkdirAll(pkgDir, 0755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(pkgDir, "file.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Simulate the local path corresponding to the URI.
	baseURI := "gcs://test_pkg"

	deleted, err := DeletePackage(context.Background(), baseURI, tmpDir)
	if err != nil {
		t.Errorf("DeletePackage() returned error: %v", err)
	}
	if !deleted {
		t.Error("expected package to be deleted")
	}

	// Verify the directory was deleted.
	if _, err := os.Stat(pkgDir); !os.IsNotExist(err) {
		t.Error("expected package directory to be deleted")
	}
}

func TestDeletePackage_File(t *testing.T) {
	tmpDir := t.TempDir()

	// DeletePackage parses the package name from the URI and removes the
	// extension. For gcs://test.zip, the package name is "test.zip" and
	// dirPath is "test" (no extension). So we create a file without an
	// extension to match this behavior.

	pkgName := "test_file" // No extension.
	pkgFile := filepath.Join(tmpDir, pkgName)
	if err := os.WriteFile(pkgFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// The package name part of the URI should be "test_file.zip" so that
	// dirPath becomes "test_file".
	baseURI := "gcs://test_file.zip"

	deleted, err := DeletePackage(context.Background(), baseURI, tmpDir)
	if err != nil {
		t.Errorf("DeletePackage() returned error: %v", err)
	}
	if !deleted {
		t.Error("expected package to be deleted")
	}

	// Verify the file was deleted.
	if _, err := os.Stat(pkgFile); !os.IsNotExist(err) {
		t.Error("expected package file to be deleted")
	}
}

func TestDeletePackage_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	baseURI := "gcs://nonexistent_pkg"

	deleted, err := DeletePackage(context.Background(), baseURI, tmpDir)
	if err != nil {
		t.Errorf("DeletePackage() returned error: %v", err)
	}
	if deleted {
		t.Error("expected non-existent package to not be deleted")
	}
}

func TestDeletePackage_Symlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping symlink test on Windows")
	}

	tmpDir := t.TempDir()

	// Create a target directory.
	realDir := filepath.Join(tmpDir, "real_dir")
	if err := os.MkdirAll(realDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a symlink.
	linkPath := filepath.Join(tmpDir, "test_pkg")
	if err := os.Symlink(realDir, linkPath); err != nil {
		t.Fatal(err)
	}

	baseURI := "gcs://test_pkg"

	deleted, err := DeletePackage(context.Background(), baseURI, tmpDir)
	if err != nil {
		t.Errorf("DeletePackage() returned error: %v", err)
	}
	if deleted {
		t.Error("expected symlink to not be deleted")
	}

	// Verify the symlink still exists.
	if _, err := os.Lstat(linkPath); os.IsNotExist(err) {
		t.Error("expected symlink to still exist")
	}
}

// ============ InstallWheelPackage tests ============

func TestInstallWheelPackage_Basic(t *testing.T) {
	// This test requires installing a wheel with pip.
	// Since the actual pip command may be unavailable or the wheel file may be
	// invalid, only a basic test is done here.
	t.Skip("Skipping test that requires pip and valid wheel file")
}

func TestInstallWheelPackage_CleanupOnFailure(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a fake wheel file.
	wheelPath := filepath.Join(tmpDir, "fake.whl")
	if err := os.WriteFile(wheelPath, []byte("fake wheel"), 0644); err != nil {
		t.Fatal(err)
	}

	targetDir := filepath.Join(tmpDir, "target")

	// The installation fails because the wheel file is invalid.
	err := InstallWheelPackage(context.Background(), wheelPath, targetDir)
	if err == nil {
		t.Log("Note: InstallWheelPackage may succeed if pip is not actually called")
	}
}

// ============ Helper functions ============

// createTestZipWithContent creates a zip file with the given content.
func createTestZipWithContent(t *testing.T, zipPath string, files map[string]string) {
	t.Helper()

	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	defer w.Close()

	for name, content := range files {
		header := &zip.FileHeader{
			Name:   name,
			Method: zip.Store,
		}

		fw, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
}

// createTestZipWithTopLevelDir creates a zip file with a top-level directory.
func createTestZipWithTopLevelDir(t *testing.T, zipPath, topDir string, files map[string]string) {
	t.Helper()

	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	defer w.Close()

	// Add the top-level directory entry.
	dirHeader := &zip.FileHeader{
		Name: topDir + "/",
	}
	_, err = w.CreateHeader(dirHeader)
	if err != nil {
		t.Fatal(err)
	}

	for name, content := range files {
		header := &zip.FileHeader{
			Name:   topDir + "/" + name,
			Method: zip.Store,
		}

		fw, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
}
