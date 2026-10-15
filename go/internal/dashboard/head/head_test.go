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

package head

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRunPublishesDashboardAddressWithoutScheme verifies that DashboardHead.Run
// publishes the dashboard address to GCS KV in scheme-less "host:port" form,
// aligned with the Python head. The Python state API client
// (ray_address_to_api_server_url in dashboard/utils.py) prepends "http://"
// itself; writing a scheme here produces "http://http://host:port" and breaks
// every ray list/summary/get state command. Regression test for the Go
// dashboard head double-scheme bug.
func TestRunPublishesDashboardAddressWithoutScheme(t *testing.T) {
	srv := newMockInternalKVServer()
	addr, stop := startMockGCS(t, srv)
	defer stop()

	client, err := NewGCSClient(context.Background(), addr, WithClusterID("0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	defer client.Close()

	cfg := &HeadConfig{
		HTTPHost:      "127.0.0.1",
		HTTPPort:      8265,
		GCSAddress:    addr,
		NodeIPAddress: "127.0.0.1",
		Minimal:       true,
		ServeFrontend: false,
	}

	head := New(cfg, client, nil)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- head.Run(ctx) }()

	// Give Run a moment to write the dashboard address KV.
	deadline := time.Now().Add(3 * time.Second)
	var stored []byte
	for time.Now().Before(deadline) {
		reply, err := client.InternalKVGet(context.Background(), "dashboard", "dashboard")
		if err == nil && reply != nil && len(reply.Value) > 0 {
			stored = reply.Value
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}

	if len(stored) == 0 {
		t.Fatal("dashboard address was not published to GCS KV")
	}
	got := string(stored)
	if strings.Contains(got, "http://") {
		t.Fatalf("dashboard address %q must be scheme-less (host:port), got a scheme", got)
	}
	if got != "127.0.0.1:8265" {
		t.Fatalf("dashboard address = %q, want %q", got, "127.0.0.1:8265")
	}
}
