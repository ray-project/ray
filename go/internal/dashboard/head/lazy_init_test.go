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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/proto"
)

// resetInitBackoff clears the OSS lazy-init cool-down cache. The INT 313d
// tests were written against a backoff-free EnsureInitialized (the INT baseline
// has no initRetryBackoff); the OSS build keeps the PR #8 cool-down, so a test
// that intentionally fails an init (e.g. the withRuntimeInitialization 500
// tests) leaves lastInitErr set and would make a later test's EnsureInitialized
// assertion return the cached error instead of running its injected
// initializer. Call at the start of any test that asserts an EnsureInitialized
// outcome to make it order-independent.
func resetInitBackoff() {
	lastInitErr = nil
}

// fakeGCSClient stubs the api.GCSClient surface the dashboard head's
// initWithConfig exercises (GetNodeToConnect + NextJobID). It lets tests drive
// the fail-closed branches without a live GCS or a linked C++ bridge.
type fakeGCSClient struct {
	getNodeInfo func(ctx context.Context, nodeIP string) (*proto.GcsNodeInfo, error)
	nextJobID   func(ctx context.Context) (string, error)
}

func (f *fakeGCSClient) GetNodeToConnect(ctx context.Context, nodeIP string) (*proto.GcsNodeInfo, error) {
	if f.getNodeInfo != nil {
		return f.getNodeInfo(ctx, nodeIP)
	}
	return nil, errors.New("GetNodeToConnect not stubbed")
}

func (f *fakeGCSClient) NextJobID(ctx context.Context) (string, error) {
	if f.nextJobID != nil {
		return f.nextJobID(ctx)
	}
	return "", errors.New("NextJobID not stubbed")
}

func (f *fakeGCSClient) Close() error   { return nil }
func (f *fakeGCSClient) IsClosed() bool { return false }

func (f *fakeGCSClient) GetPlacementGroupInfo(ctx context.Context, id ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, errors.New("GetPlacementGroupInfo not stubbed")
}
func (f *fakeGCSClient) GetPlacementGroupInfoByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, errors.New("GetPlacementGroupInfoByName not stubbed")
}
func (f *fakeGCSClient) GetAllPlacementGroupInfo(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, errors.New("GetAllPlacementGroupInfo not stubbed")
}
func (f *fakeGCSClient) GetInternalKV(ctx context.Context, ns, key string) ([]byte, error) {
	return nil, errors.New("GetInternalKV not stubbed")
}
func (f *fakeGCSClient) GetAllNodeInfo(ctx context.Context) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, errors.New("GetAllNodeInfo not stubbed")
}
func (f *fakeGCSClient) GetAllActorInfo(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, errors.New("GetAllActorInfo not stubbed")
}

// runWithFakeGCS runs fn with withCachedClientFn swapped for a stub that hands
// fn the given fake client, restoring the original after.
func runWithFakeGCS(fake api.GCSClient, fn func()) {
	orig := withCachedClientFn
	withCachedClientFn = func(gcsAddress string, opts gcs.ClientOptions, cb func(api.GCSClient) error) error {
		return cb(fake)
	}
	defer func() { withCachedClientFn = orig }()
	fn()
}

// TestInitWithConfigFailsClosedOnGetNodeToConnectError verifies that a
// GetNodeToConnect RPC failure surfaces an error from initWithConfig and the
// preflight (and thus any dial) is never reached: without node information the
// sockets stay empty and the node manager port stays 0, which base.Initialize
// would hand to CoreWorkerProcess as an empty raylet socket (RAY_LOG(FATAL))
// or a port-0 address (QuickExit) killing the whole dashboard.
func TestInitWithConfigFailsClosedOnGetNodeToConnectError(t *testing.T) {
	sentinel := errors.New("gcs rpc failed")
	runWithFakeGCS(&fakeGCSClient{getNodeInfo: func(ctx context.Context, nodeIP string) (*proto.GcsNodeInfo, error) {
		return nil, sentinel
	}}, func() {
		origPre := preflightConnectFn
		called := false
		preflightConnectFn = func(addr string, timeout time.Duration) error { called = true; return nil }
		defer func() { preflightConnectFn = origPre }()

		cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
		err := initWithConfig(cfg)
		if err == nil {
			t.Fatal("initWithConfig returned nil, want error for GetNodeToConnect failure")
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("initWithConfig error = %v, want sentinel %v", err, sentinel)
		}
		if called {
			t.Fatal("preflightConnectFn called despite GetNodeToConnect failure; want fail-closed before any dial")
		}
	})
}

// TestInitWithConfigFailsClosedOnNilNodeInfo verifies that a successful
// GetNodeToConnect returning no node info also fails closed: an empty node
// would leave sockets empty and NodeManagerPort=0, the same QuickExit/FATAL
// hazards as an RPC error.
func TestInitWithConfigFailsClosedOnNilNodeInfo(t *testing.T) {
	runWithFakeGCS(&fakeGCSClient{getNodeInfo: func(ctx context.Context, nodeIP string) (*proto.GcsNodeInfo, error) {
		return nil, nil
	}}, func() {
		cfg := &HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"}
		err := initWithConfig(cfg)
		if err == nil {
			t.Fatal("initWithConfig returned nil for empty node info, want error (fail-closed)")
		}
		if !errors.Is(err, errNoNodeInfo) {
			t.Fatalf("initWithConfig error = %v, want ErrNoNodeInfo", err)
		}
	})
}

// TestEnsureInitializedCallsInitOnce verifies the uninitialized path invokes
// the initializer exactly once per call (the test binary does not load
// go_runtime.so, so api.IsInitialized() is always false here).
func TestEnsureInitializedCallsInitOnce(t *testing.T) {
	resetInitBackoff()
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
	resetInitBackoff()
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
	resetInitBackoff()
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

// TestPreflightNodeManagerRejectsUnreachable verifies the preflight fails
// closed: a node_manager_port that refuses TCP connections (the stale
// GCS-recovered head node case) must surface an error so the caller degrades
// instead of letting base.Initialize dial a dead port and QuickExit.
func TestPreflightNodeManagerRejectsUnreachable(t *testing.T) {
	sentinel := errors.New("connection refused")
	orig := preflightConnectFn
	preflightConnectFn = func(addr string, timeout time.Duration) error { return sentinel }
	defer func() { preflightConnectFn = orig }()

	err := preflightNodeManager("10.0.0.1", 12345)
	if err == nil {
		t.Fatal("preflightNodeManager returned nil, want error for unreachable port")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("preflightNodeManager error = %v, want sentinel %v", err, sentinel)
	}
}

// TestPreflightNodeManagerAllowsReachable verifies a reachable port passes.
func TestPreflightNodeManagerAllowsReachable(t *testing.T) {
	orig := preflightConnectFn
	preflightConnectFn = func(addr string, timeout time.Duration) error { return nil }
	defer func() { preflightConnectFn = orig }()

	if err := preflightNodeManager("10.0.0.1", 12345); err != nil {
		t.Fatalf("preflightNodeManager returned error for reachable port: %v", err)
	}
}

// TestPreflightNodeManagerSkipsUnset verifies the preflight is a no-op when
// GCS returned no usable port (so callers that intentionally skip dialing are
// not blocked).
func TestPreflightNodeManagerSkipsUnset(t *testing.T) {
	called := false
	orig := preflightConnectFn
	preflightConnectFn = func(addr string, timeout time.Duration) error {
		called = true
		return nil
	}
	defer func() { preflightConnectFn = orig }()

	for _, tc := range []struct {
		name string
		ip   string
		port int32
	}{
		{"zero port", "10.0.0.1", 0},
		{"empty ip", "", 12345},
	} {
		if err := preflightNodeManager(tc.ip, tc.port); err != nil {
			t.Fatalf("%s: preflightNodeManager returned error: %v", tc.name, err)
		}
	}
	if called {
		t.Fatal("preflightConnectFn called for unset node_manager_port; want no-op")
	}
}
