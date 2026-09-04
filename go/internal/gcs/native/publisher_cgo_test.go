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

//go:build cgo

package native

import (
	"context"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

func TestNewLogBatchPublisher_NilClient(t *testing.T) {
	var client gcs.Client
	pub, err := NewLogBatchPublisher(client)
	if pub != nil {
		t.Errorf("expected nil publisher for nil client, got %v", pub)
	}
	if err == nil {
		t.Fatal("expected error for nil client, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported gcs client type") {
		t.Errorf("expected 'unsupported gcs client type' in error, got %q", err)
	}
}

func TestNewLogBatchPublisher_UnsupportedClient(t *testing.T) {
	// gcs.NewClient returns a non-*cgoClient only via a stub; a typed value
	// that is not *cgoClient always fails the type assertion.
	client := dummyNonNativeClient{}
	pub, err := NewLogBatchPublisher(client)
	if pub != nil {
		t.Errorf("expected nil publisher for unsupported client, got %v", pub)
	}
	if err == nil {
		t.Fatal("expected error for unsupported client type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported gcs client type") {
		t.Errorf("expected 'unsupported gcs client type' in error, got %q", err)
	}
}

// dummyNonNativeClient is an unrelated concrete type used to prove that
// NewLogBatchPublisher rejects anything that is not a *cgoClient.
type dummyNonNativeClient struct {
	gcs.Client
}

func TestLogBatchPublisher_PublishLogBatch_NilReceiver(t *testing.T) {
	var pub *LogBatchPublisher
	payload := gcs.LogBatchPayload{
		IP:    "127.0.0.1",
		PID:   "1",
		JobID: "job",
		Lines: []string{"line1"},
	}
	err := pub.PublishLogBatch(context.Background(), payload)
	if err == nil {
		t.Fatal("expected error for nil receiver, got nil")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("expected 'not initialized' in error, got %q", err)
	}
}

func TestLogBatchPublisher_PublishLogBatch_NilClientPointer(t *testing.T) {
	// A publisher holding a nil *cgoClient is rejected before any C call.
	pub := &LogBatchPublisher{client: nil}
	payload := gcs.LogBatchPayload{
		IP:    "127.0.0.1",
		PID:   "1",
		JobID: "job",
		Lines: []string{"line1"},
	}
	err := pub.PublishLogBatch(context.Background(), payload)
	if err == nil {
		t.Fatal("expected error for uninitialized client, got nil")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("expected 'not initialized' in error, got %q", err)
	}
}

func TestLogBatchPublisher_PublishLogBatch_NullBackingClient(t *testing.T) {
	// A *cgoClient with a nil backing C client reaches the C++ boundary,
	// which rejects the null client and reports an error. This exercises the
	// fail path of publish across the cgo bridge without a live GCS cluster.
	pub := &LogBatchPublisher{client: &cgoClient{}}
	payload := gcs.LogBatchPayload{
		IP:    "127.0.0.1",
		PID:   "1",
		JobID: "job",
		Lines: []string{"line1"},
	}
	err := pub.PublishLogBatch(context.Background(), payload)
	if err == nil {
		t.Fatal("expected error from null backing client, got nil")
	}
	t.Logf("publish against null backing client failed as expected: %v", err)
}

func Test_boolToInt(t *testing.T) {
	if got := boolToInt(true); got != 1 {
		t.Errorf("boolToInt(true) = %d, want 1", got)
	}
	if got := boolToInt(false); got != 0 {
		t.Errorf("boolToInt(false) = %d, want 0", got)
	}
}
