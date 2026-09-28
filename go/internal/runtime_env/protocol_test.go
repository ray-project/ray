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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ============ Protocol type tests ============

func TestProtocolConstants(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		expected string
	}{
		{"GCS", ProtocolGCS, "gcs"},
		{"Conda", ProtocolConda, "conda"},
		{"Pip", ProtocolPip, "pip"},
		{"Uv", ProtocolUv, "uv"},
		{"HTTPS", ProtocolHTTPS, "https"},
		{"S3", ProtocolS3, "s3"},
		{"GS", ProtocolGS, "gs"},
		{"Azure", ProtocolAzure, "azure"},
		{"ABFSS", ProtocolABFSS, "abfss"},
		{"File", ProtocolFile, "file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.protocol) != tt.expected {
				t.Errorf("expected %s protocol to be %q, got %q", tt.name, tt.expected, tt.protocol)
			}
		})
	}
}

// ============ GetProtocols tests ============

func TestGetProtocols(t *testing.T) {
	protocols := GetProtocols()

	// The base protocols are always advertised, in declaration order. Cloud
	// protocols (s3/gs/azure/abfss) are appended only when their handlers are
	// compiled in (the "cloud" build tag), so expectedProtocols mirrors
	// GetProtocols' base-then-cloud ordering in every build.
	expectedProtocols := append([]Protocol{
		ProtocolGCS,
		ProtocolConda,
		ProtocolPip,
		ProtocolUv,
		ProtocolHTTPS,
		ProtocolFile,
	}, cloudProtocols()...)

	if len(protocols) != len(expectedProtocols) {
		t.Errorf("expected %d protocols, got %d", len(expectedProtocols), len(protocols))
	}

	for i, expected := range expectedProtocols {
		if i >= len(protocols) {
			t.Errorf("missing protocol at index %d: %s", i, expected)
			continue
		}
		if protocols[i] != expected {
			t.Errorf("expected protocol at index %d to be %s, got %s", i, expected, protocols[i])
		}
	}
}

// ============ GetRemoteProtocols tests ============

func TestGetRemoteProtocols(t *testing.T) {
	remoteProtocols := GetRemoteProtocols()

	expectedRemoteProtocols := append([]Protocol{
		ProtocolHTTPS,
		ProtocolFile,
	}, cloudProtocols()...)

	if len(remoteProtocols) != len(expectedRemoteProtocols) {
		t.Errorf("expected %d remote protocols, got %d", len(expectedRemoteProtocols), len(remoteProtocols))
	}

	for i, expected := range expectedRemoteProtocols {
		if i >= len(remoteProtocols) {
			t.Errorf("missing remote protocol at index %d: %s", i, expected)
			continue
		}
		if remoteProtocols[i] != expected {
			t.Errorf("expected remote protocol at index %d to be %s, got %s", i, expected, remoteProtocols[i])
		}
	}
}

// ============ IsRemoteProtocol tests ============

func TestIsRemoteProtocol_True(t *testing.T) {
	for _, protocol := range GetRemoteProtocols() {
		t.Run(string(protocol), func(t *testing.T) {
			if !IsRemoteProtocol(protocol) {
				t.Errorf("IsRemoteProtocol(%q) should return true", protocol)
			}
		})
	}
}

func TestIsRemoteProtocol_False(t *testing.T) {
	localProtocols := []Protocol{
		ProtocolGCS,
		ProtocolConda,
		ProtocolPip,
		ProtocolUv,
	}

	for _, protocol := range localProtocols {
		t.Run(string(protocol), func(t *testing.T) {
			if IsRemoteProtocol(protocol) {
				t.Errorf("IsRemoteProtocol(%q) should return false", protocol)
			}
		})
	}
}

// TestDefaultBuildDoesNotAdvertiseCloudProtocols guards the F7 invariant:
// in a build without the "cloud" tag the cloud download handlers are stubs
// that always fail, so their protocols must not be advertised by
// GetProtocols/GetRemoteProtocols nor pass URI validation.
func TestDefaultBuildDoesNotAdvertiseCloudProtocols(t *testing.T) {
	cloud := []Protocol{ProtocolS3, ProtocolGS, ProtocolAzure, ProtocolABFSS}
	if len(cloudProtocols()) != 0 {
		t.Skip("cloud build: cloud protocols are advertized by design")
	}

	for _, protocol := range cloud {
		if IsRemoteProtocol(protocol) {
			t.Errorf("IsRemoteProtocol(%q) should be false without cloud handlers", protocol)
		}
		if isValidProtocol(protocol) {
			t.Errorf("isValidProtocol(%q) should be false without cloud handlers", protocol)
		}
	}
}

// ============ isValidProtocol tests ============

func TestIsValidProtocol_True(t *testing.T) {
	validProtocols := GetProtocols()

	for _, protocol := range validProtocols {
		t.Run(string(protocol), func(t *testing.T) {
			if !isValidProtocol(protocol) {
				t.Errorf("isValidProtocol(%q) should return true", protocol)
			}
		})
	}
}

func TestIsValidProtocol_False(t *testing.T) {
	invalidProtocols := []Protocol{
		"invalid",
		"unknown",
		"http",
		"ftp",
		"",
	}

	for _, protocol := range invalidProtocols {
		t.Run(string(protocol), func(t *testing.T) {
			if isValidProtocol(protocol) {
				t.Errorf("isValidProtocol(%q) should return false", protocol)
			}
		})
	}
}

// ============ DownloadOptions tests ============

func TestDefaultDownloadOptions(t *testing.T) {
	if DefaultDownloadOptions == nil {
		t.Fatal("expected DefaultDownloadOptions to be initialized")
	}

	expectedTimeout := 300 * time.Second
	if DefaultDownloadOptions.HTTPTimeout != expectedTimeout {
		t.Errorf("expected HTTPTimeout to be %v, got %v", expectedTimeout, DefaultDownloadOptions.HTTPTimeout)
	}

	if DefaultDownloadOptions.BufferSize != 32*1024 {
		t.Errorf("expected BufferSize to be 32KB, got %d", DefaultDownloadOptions.BufferSize)
	}

	if DefaultDownloadOptions.UseAtomicWrite {
		t.Error("expected UseAtomicWrite to be false by default")
	}
}

// ============ Downloader tests ============

func TestNewDownloader_NilOptions(t *testing.T) {
	downloader := NewDownloader(nil)
	if downloader == nil {
		t.Fatal("expected NewDownloader to return non-nil downloader")
	}
	if downloader.defaultOpts != DefaultDownloadOptions {
		t.Error("expected defaultOpts to be DefaultDownloadOptions")
	}
	if len(downloader.handlers) != 6 {
		t.Errorf("expected 6 handlers, got %d", len(downloader.handlers))
	}

	// Verify handlers are in correct order
	if _, ok := downloader.handlers[0].(*fileProtocolHandler); !ok {
		t.Error("expected first handler to be fileProtocolHandler")
	}
	if _, ok := downloader.handlers[1].(*gsProtocolHandler); !ok {
		t.Error("expected second handler to be gsProtocolHandler")
	}
	if _, ok := downloader.handlers[2].(*s3ProtocolHandler); !ok {
		t.Error("expected third handler to be s3ProtocolHandler")
	}
	if _, ok := downloader.handlers[3].(*azureProtocolHandler); !ok {
		t.Error("expected fourth handler to be azureProtocolHandler")
	}
	if _, ok := downloader.handlers[4].(*abfssProtocolHandler); !ok {
		t.Error("expected fifth handler to be abfssProtocolHandler")
	}
	if _, ok := downloader.handlers[5].(*httpProtocolHandler); !ok {
		t.Error("expected sixth handler to be httpProtocolHandler")
	}
}

func TestNewDownloader_WithOptions(t *testing.T) {
	customOpts := &DownloadOptions{
		HTTPTimeout:    60 * time.Second,
		BufferSize:     64 * 1024,
		UseAtomicWrite: true,
	}

	downloader := NewDownloader(customOpts)
	if downloader == nil {
		t.Fatal("expected NewDownloader to return non-nil downloader")
	}
	if downloader.defaultOpts != customOpts {
		t.Error("expected defaultOpts to be customOpts")
	}
}

func TestDownloader_Download_NonRemoteProtocol(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	downloader := NewDownloader(nil)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolGCS, "gcs://bucket/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for non-remote protocol")
	}
	if !strings.Contains(err.Error(), "is not a remote protocol") {
		t.Errorf("expected 'is not a remote protocol' error, got %v", err)
	}
}

func TestDownloader_Download_FileProtocol_Success(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	downloader := NewDownloader(nil)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, nil)
	if err != nil {
		t.Errorf("Download() returned error: %v", err)
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "test content" {
		t.Errorf("expected 'test content', got %q", string(content))
	}
}

func TestDownloader_Download_FileProtocol_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file:///nonexistent/source.txt"

	downloader := NewDownloader(nil)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, nil)
	if err == nil {
		t.Error("expected error for non-existent source file")
	}
}

func TestDownloader_Download_FileProtocol_EscapedPath(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source with spaces.txt")
	if err := os.WriteFile(srcFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + filepath.ToSlash(srcFile)

	downloader := NewDownloader(nil)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, nil)
	if err != nil {
		t.Errorf("Download() returned error: %v", err)
	}
}

func TestDownloader_Download_AtomicWrite_Success(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("atomic test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	opts := &DownloadOptions{
		UseAtomicWrite: true,
		BufferSize:     32 * 1024,
	}

	downloader := NewDownloader(opts)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, opts)
	if err != nil {
		t.Errorf("Download() with atomic write returned error: %v", err)
		return
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "atomic test content" {
		t.Errorf("expected 'atomic test content', got %q", string(content))
	}

	matches, _ := filepath.Glob(filepath.Join(tmpDir, ".download-*"))
	if len(matches) > 0 {
		t.Error("expected temporary files to be cleaned up after atomic write")
	}
}

func TestDownloader_Download_AtomicWrite_CleanupOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file:///nonexistent/source.txt"

	opts := &DownloadOptions{
		UseAtomicWrite: true,
	}

	downloader := NewDownloader(opts)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, opts)
	if err == nil {
		t.Error("expected error for non-existent source file")
	}

	matches, _ := filepath.Glob(filepath.Join(tmpDir, ".download-*"))
	if len(matches) > 0 {
		t.Error("expected temporary files to be cleaned up on failure")
	}
}

func TestDownloader_Download_CustomBufferSize(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("buffered test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	opts := &DownloadOptions{
		BufferSize: 64 * 1024,
	}

	downloader := NewDownloader(opts)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, opts)
	if err != nil {
		t.Errorf("Download() with custom buffer size returned error: %v", err)
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "buffered test content" {
		t.Errorf("expected 'buffered test content', got %q", string(content))
	}
}

// ============ DownloadRemoteURI tests ============

func TestDownloadRemoteURI_NilOptions(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolHTTPS, "https://example.com/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for network request (may fail in test environment)")
	}
}

func TestDownloadRemoteURI_WithCustomOptions(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	opts := &DownloadOptions{
		HTTPTimeout: 10 * time.Second,
	}

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolHTTPS, "https://example.com/file.zip", destFile, opts)
	if err == nil {
		t.Error("expected error for network request (may fail in test environment)")
	}
}

func TestDownloadRemoteURI_InvalidProtocol(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolGCS, "gcs://bucket/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for non-remote protocol")
	}
	if !strings.Contains(err.Error(), "is not a remote protocol") {
		t.Errorf("expected 'is not a remote protocol' error, got %v", err)
	}
}

func TestDownloadRemoteURI_FileProtocol_Success(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolFile, uri, destFile, nil)
	if err != nil {
		t.Errorf("DownloadRemoteURI() returned error: %v", err)
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "test content" {
		t.Errorf("expected 'test content', got %q", string(content))
	}
}

func TestDownloadRemoteURI_FileProtocol_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file:///nonexistent/source.txt"

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolFile, uri, destFile, nil)
	if err == nil {
		t.Error("expected error for non-existent source file")
	}
}

func TestDownloadRemoteURI_FileProtocol_EscapedPath(t *testing.T) {
	tmpDir := t.TempDir()

	srcFile := filepath.Join(tmpDir, "source with spaces.txt")
	if err := os.WriteFile(srcFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + filepath.ToSlash(srcFile)

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolFile, uri, destFile, nil)
	if err != nil {
		t.Errorf("DownloadRemoteURI() returned error: %v", err)
	}
}

func TestDownloadRemoteURI_S3Protocol_NoCredentials(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolS3, "s3://bucket/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for S3 protocol without proper mocking")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("S3 protocol should be implemented, got error: %v", err)
	}
}

func TestDownloadRemoteURI_GSProtocol_NoCredentials(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolGS, "gs://bucket/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for GS protocol without proper credentials")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("GS protocol should be implemented, got error: %v", err)
	}
}

func TestDownloadRemoteURI_ABFSSProtocol_NoCredentials(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolABFSS, "abfss://container/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for ABFSS protocol without proper credentials")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("ABFSS protocol should be implemented, got error: %v", err)
	}
}

// ============ Protocol handler tests ============

func TestFileProtocolHandler_Match(t *testing.T) {
	handler := &fileProtocolHandler{}

	if !handler.Match(ProtocolFile) {
		t.Error("fileProtocolHandler should match ProtocolFile")
	}

	if handler.Match(ProtocolHTTPS) {
		t.Error("fileProtocolHandler should not match ProtocolHTTPS")
	}
}

func TestS3ProtocolHandler_Match(t *testing.T) {
	handler := &s3ProtocolHandler{}

	if !handler.Match(ProtocolS3) {
		t.Error("s3ProtocolHandler should match ProtocolS3")
	}

	if handler.Match(ProtocolGS) {
		t.Error("s3ProtocolHandler should not match ProtocolGS")
	}
}

func TestGSProtocolHandler_Match(t *testing.T) {
	handler := &gsProtocolHandler{}

	if !handler.Match(ProtocolGS) {
		t.Error("gsProtocolHandler should match ProtocolGS")
	}

	if handler.Match(ProtocolS3) {
		t.Error("gsProtocolHandler should not match ProtocolS3")
	}
}

func TestAzureProtocolHandler_Match(t *testing.T) {
	handler := &azureProtocolHandler{}

	if !handler.Match(ProtocolAzure) {
		t.Error("azureProtocolHandler should match ProtocolAzure")
	}

	if handler.Match(ProtocolABFSS) {
		t.Error("azureProtocolHandler should not match ProtocolABFSS")
	}
}

func TestABFSSProtocolHandler_Match(t *testing.T) {
	handler := &abfssProtocolHandler{}

	if !handler.Match(ProtocolABFSS) {
		t.Error("abfssProtocolHandler should match ProtocolABFSS")
	}

	if handler.Match(ProtocolAzure) {
		t.Error("abfssProtocolHandler should not match ProtocolAzure")
	}
}

func TestHTTPProtocolHandler_Match(t *testing.T) {
	handler := &httpProtocolHandler{}

	// httpProtocolHandler.Match() always returns true as it's the default handler
	protocols := []Protocol{
		ProtocolHTTPS,
		ProtocolS3,
		ProtocolGS,
		ProtocolAzure,
		ProtocolABFSS,
		ProtocolFile,
		ProtocolGCS,
		ProtocolConda,
		ProtocolPip,
		ProtocolUv,
	}

	for _, protocol := range protocols {
		t.Run(string(protocol), func(t *testing.T) {
			if !handler.Match(protocol) {
				t.Errorf("httpProtocolHandler should match %s", protocol)
			}
		})
	}
}

// ============ bufio.Writer integration tests ============

func TestBufferedWriterIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	testContent := []byte("buffered writer integration test content")

	if err := os.WriteFile(srcFile, testContent, 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	opts := &DownloadOptions{
		BufferSize: 16, // A small buffer so the test exercises multiple writes.
	}

	downloader := NewDownloader(opts)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, opts)
	if err != nil {
		t.Errorf("Download() with buffered writer returned error: %v", err)
		return
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, testContent) {
		t.Errorf("expected %q, got %q", string(testContent), string(content))
	}
}

func TestBufferedWriterFlushBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")

	// Create content larger than the buffer to exercise flushing.
	testContent := make([]byte, 100)
	for i := range testContent {
		testContent[i] = byte('A' + (i % 26))
	}

	if err := os.WriteFile(srcFile, testContent, 0644); err != nil {
		t.Fatal(err)
	}

	destFile := filepath.Join(tmpDir, "dest.txt")
	uri := "file://" + srcFile

	opts := &DownloadOptions{
		BufferSize: 32, // A small buffer forces multiple flushes.
	}

	downloader := NewDownloader(opts)
	ctx := context.Background()

	err := downloader.Download(ctx, ProtocolFile, uri, destFile, opts)
	if err != nil {
		t.Errorf("Download() with small buffer returned error: %v", err)
		return
	}

	content, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, testContent) {
		t.Error("content mismatch after buffered write with multiple flushes")
	}
}
