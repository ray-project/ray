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

//go:build cloud

package runtime_env

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOperationProtocolsAdvertised verifies that the cloud build advertises
// exactly the cloud protocols whose handlers are compiled in, so GetProtocols
// and URI validation accept every downloadable protocol.
func TestOperationProtocolsAdvertised(t *testing.T) {
	expected := []Protocol{ProtocolS3, ProtocolGS, ProtocolAzure, ProtocolABFSS}

	got := cloudProtocols()
	missing := make(map[Protocol]bool)
	for _, p := range expected {
		missing[p] = true
	}
	for _, p := range got {
		if missing[p] {
			delete(missing, p)
		}
	}
	for p := range missing {
		t.Errorf("cloudProtocols() missing %q", p)
	}

	for _, p := range expected {
		if !IsRemoteProtocol(p) {
			t.Errorf("IsRemoteProtocol(%q) should be true in the cloud build", p)
		}
	}
	if !isValidProtocol(ProtocolS3) || !isValidProtocol(ProtocolAzure) {
		t.Error("cloud protocols should pass validation in the cloud build")
	}
}

// TestNewDownloader_S3HandlerCache verifies that the S3 handler registered by
// NewDownloader owns an initialized client cache.
func TestNewDownloader_S3HandlerCache(t *testing.T) {
	downloader := NewDownloader(nil)

	handler, ok := downloader.handlers[2].(*s3ProtocolHandler)
	if !ok {
		t.Fatal("expected third handler to be s3ProtocolHandler")
	}
	if handler.clientCache == nil {
		t.Error("expected s3 handler clientCache to be initialized")
	}
}

// ============ DownloadRemoteURI tests ============

func TestDownloadRemoteURI_AzureProtocol_MissingCredentials(t *testing.T) {
	tmpDir := t.TempDir()
	destFile := filepath.Join(tmpDir, "test.txt")

	ctx := context.Background()
	err := DownloadRemoteURI(ctx, ProtocolAzure, "azure://container/file.zip", destFile, nil)
	if err == nil {
		t.Error("expected error for Azure protocol without credentials")
	}
	if !strings.Contains(err.Error(), "AZURE_STORAGE_ACCOUNT") {
		t.Errorf("expected 'AZURE_STORAGE_ACCOUNT' error, got %v", err)
	}
}

// ============ S3 client cache tests ============

func TestS3ClientCache_UnauthenticatedClient(t *testing.T) {
	cache := newS3ClientCache(300 * time.Second)

	client := cache.getUnauthenticatedClient()
	if client == nil {
		t.Error("expected unauthenticated client to be created")
	}

	sameClient := cache.getUnauthenticatedClient()
	if sameClient != client {
		t.Error("expected cached client to be returned")
	}
}

func TestS3ClientCache_AuthenticatedClient_NoConfig(t *testing.T) {
	cache := newS3ClientCache(300 * time.Second)
	ctx := context.Background()

	// Try to obtain an authenticated client without AWS configuration.
	// Note: this test is environment dependent; some environments have default AWS configuration.
	client, err := cache.getAuthenticatedClient(ctx)
	if err != nil {
		// Without AWS configuration this should return an error.
		t.Logf("Got expected error: %v", err)
	} else if client == nil {
		t.Error("expected non-nil client when no error is returned")
	}
	// With AWS configuration present, the test passes and returns a valid client.
}
