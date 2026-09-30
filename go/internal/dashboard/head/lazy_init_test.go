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

package head

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEnsureInitializedCallsInitOnce verifies the uninitialized path invokes
// the initializer exactly once per call (the test binary does not load
// go_runtime.so, so api.IsInitialized() is always false here).
func TestEnsureInitializedCallsInitOnce(t *testing.T) {
	var calls atomic.Int32
	orig := initWithConfigFn
	initWithConfigFn = func(cfg *HeadConfig) error {
		calls.Add(1)
		return nil
	}
	defer func() { initWithConfigFn = orig }()

	cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
	if err := EnsureInitialized(cfg); err != nil {
		t.Fatalf("EnsureInitialized returned error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("initWithConfig called %d times, want 1", calls.Load())
	}
	if err := EnsureInitialized(cfg); err != nil {
		t.Fatalf("second EnsureInitialized returned error: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("initWithConfig called %d times total, want 2", calls.Load())
	}
}

// TestEnsureInitializedConcurrentNoPanic verifies that concurrent callers
// serialize through initOnce and never trigger the setHandle "already
// initialized" panic, even when every caller observes an uninitialized runtime
// and would otherwise race into InitWithOptions.
func TestEnsureInitializedConcurrentNoPanic(t *testing.T) {
	orig := initWithConfigFn
	initWithConfigFn = func(cfg *HeadConfig) error { return nil }
	defer func() { initWithConfigFn = orig }()

	cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
	var wg sync.WaitGroup
	errs := make([]error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = EnsureInitialized(cfg)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent EnsureInitialized[%d] returned error: %v", i, err)
		}
	}
}

// TestEnsureInitializedPropagatesInitError verifies an initialization failure
// is returned to the caller so the calling module can degrade.
func TestEnsureInitializedPropagatesInitError(t *testing.T) {
	sentinel := errors.New("gcs not ready")
	orig := initWithConfigFn
	initWithConfigFn = func(cfg *HeadConfig) error { return sentinel }
	defer func() { initWithConfigFn = orig }()

	cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
	err := EnsureInitialized(cfg)
	if !errors.Is(err, sentinel) {
		t.Fatalf("EnsureInitialized error = %v, want sentinel %v", err, sentinel)
	}
}

// TestEnsureInitializedBackoffAfterFailure verifies that a failed lazy init
// enters a cool-down: within initRetryBackoff subsequent calls return the
// cached error without re-entering the initializer. Every attempt constructs
// the C++ CoreWorker, and a CoreWorker that cannot reach the local raylet
// exits the whole process from the C++ side, so the retry must not run per
// request.
func TestEnsureInitializedBackoffAfterFailure(t *testing.T) {
	sentinel := errors.New("raylet unreachable")
	var calls atomic.Int32
	orig := initWithConfigFn
	initWithConfigFn = func(cfg *HeadConfig) error {
		calls.Add(1)
		return sentinel
	}
	defer func() { initWithConfigFn = orig }()
	origBackoff := initRetryBackoff
	initRetryBackoff = time.Minute
	defer func() { initRetryBackoff = origBackoff }()
	lastInitErr = nil

	cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
	if err := EnsureInitialized(cfg); !errors.Is(err, sentinel) {
		t.Fatalf("first EnsureInitialized error = %v, want sentinel", err)
	}
	if err := EnsureInitialized(cfg); !errors.Is(err, sentinel) {
		t.Fatalf("second EnsureInitialized (within backoff) error = %v, want sentinel", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("initWithConfig called %d times within backoff, want 1 (no re-entry)", calls.Load())
	}
}

// TestEnsureInitializedRetriesAfterBackoff verifies the cool-down expires and
// a retry is allowed, preserving the recover-on-next-request design for a GCS
// or raylet that comes back.
func TestEnsureInitializedRetriesAfterBackoff(t *testing.T) {
	sentinel := errors.New("gcs not ready")
	var calls atomic.Int32
	orig := initWithConfigFn
	initWithConfigFn = func(cfg *HeadConfig) error {
		calls.Add(1)
		return sentinel
	}
	defer func() { initWithConfigFn = orig }()
	origBackoff := initRetryBackoff
	initRetryBackoff = 0
	defer func() { initRetryBackoff = origBackoff }()
	lastInitErr = nil

	cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
	if err := EnsureInitialized(cfg); !errors.Is(err, sentinel) {
		t.Fatalf("first EnsureInitialized error = %v, want sentinel", err)
	}
	if err := EnsureInitialized(cfg); !errors.Is(err, sentinel) {
		t.Fatalf("second EnsureInitialized (backoff expired) error = %v, want sentinel", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("initWithConfig called %d times after backoff expiry, want 2", calls.Load())
	}
}
