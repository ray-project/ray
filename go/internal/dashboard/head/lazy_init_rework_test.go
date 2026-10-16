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
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// fakeHandshakeClient implements enough of gcs.Client for the handshake.
type fakeHandshakeClient struct {
	gcs.Client
	nodeInfo *proto.GcsNodeInfo
	jobID    string
	closed   bool
}

func (f *fakeHandshakeClient) GetNodeToConnect(ctx context.Context, nodeIP string) (*proto.GcsNodeInfo, error) {
	return f.nodeInfo, nil
}

func (f *fakeHandshakeClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.JobIDFromHex(f.jobID)
}

func (f *fakeHandshakeClient) Close() error {
	f.closed = true
	return nil
}

func (f *fakeHandshakeClient) IsClosed() bool { return f.closed }

func TestInitWithConfigConstructsOptions(t *testing.T) {
	// The merged dashboard preflight would dial the fake NodeManagerPort and
	// fail (no real raylet listens in tests); stub it to pass.
	origPre := preflightConnectFn
	preflightConnectFn = func(addr string, timeout time.Duration) error { return nil }
	defer func() { preflightConnectFn = origPre }()

	dir := t.TempDir()
	// ports_by_node.json so the metrics agent port is reused.
	nodeIP := "127.0.0.1"
	storeSocket := "mock_store"
	if err := os.WriteFile(filepath.Join(dir, "ports_by_node.json"),
		[]byte(`{"`+nodeIP+`:`+storeSocket+`":{"metrics_agent_port":43215}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	handshake := &fakeHandshakeClient{
		nodeInfo: &proto.GcsNodeInfo{
			ObjectStoreSocketName: storeSocket,
			RayletSocketName:      "mock_raylet",
			NodeManagerPort:       12345,
			NodeManagerAddress:    "127.0.0.1",
		},
		jobID: "01020304",
	}

	origLoad := loadGCSClientFn
	loadGCSClientFn = func(opts gcs.ClientOptions) (gcs.Client, error) { return handshake, nil }
	defer func() { loadGCSClientFn = origLoad }()

	var captured *options.InitializeOptions
	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(opts *options.InitializeOptions) error {
		copied := *opts
		captured = &copied
		return nil
	}
	defer func() { initRuntimeWithOptionsFn = origInit }()

	cfg := &HeadConfig{
		GCSAddress:     "127.0.0.1:6379",
		ClusterIDHex:   "abc",
		NodeIPAddress:  nodeIP,
		SessionDir:     dir,
		CodeSearchPath: []string{"/path/to/code"},
	}
	if err := initWithConfig(cfg); err != nil {
		t.Fatalf("initWithConfig error = %v", err)
	}

	if captured == nil {
		t.Fatal("initRuntimeWithOptionsFn not called")
	}
	if captured.WorkerType != options.WorkerTypeDriver {
		t.Fatalf("WorkerType = %v, want Driver", captured.WorkerType)
	}
	if captured.Runtime.StoreSocket != storeSocket {
		t.Fatalf("StoreSocket = %q, want %q", captured.Runtime.StoreSocket, storeSocket)
	}
	if captured.Runtime.RayletSocket != "mock_raylet" {
		t.Fatalf("RayletSocket = %q, want mock_raylet", captured.Runtime.RayletSocket)
	}
	if captured.Network.NodeManagerPort != 12345 {
		t.Fatalf("NodeManagerPort = %d, want 12345", captured.Network.NodeManagerPort)
	}
	if captured.Runtime.MetricsAgentPort != 43215 {
		t.Fatalf("MetricsAgentPort = %d, want 43215", captured.Runtime.MetricsAgentPort)
	}
	if captured.Job.JobID != "01020304" {
		t.Fatalf("JobID = %q, want 01020304", captured.Job.JobID)
	}
	if !captured.SkipDriverLogSetup {
		t.Fatal("SkipDriverLogSetup = false, want true (dashboard configures its own logger)")
	}
	if captured.Job.JobConfig == "" {
		t.Fatal("JobConfig empty, want the _ray_internal_dashboard JobConfig")
	}
	decoded, err := base64.StdEncoding.DecodeString(captured.Job.JobConfig)
	if err != nil {
		t.Fatalf("decode JobConfig: %v", err)
	}
	var jc proto.JobConfig
	if err := protolib.Unmarshal(decoded, &jc); err != nil {
		t.Fatalf("unmarshal JobConfig: %v", err)
	}
	if jc.RayNamespace != "_ray_internal_dashboard" {
		t.Fatalf("ray_namespace = %q, want _ray_internal_dashboard", jc.RayNamespace)
	}

	if !handshake.closed {
		t.Fatal("handshake client not closed after use")
	}
}

func TestInitWithConfigPropagatesLoadError(t *testing.T) {
	sentinel := errors.New("plugin load failed")
	origLoad := loadGCSClientFn
	loadGCSClientFn = func(gcs.ClientOptions) (gcs.Client, error) { return nil, sentinel }
	defer func() { loadGCSClientFn = origLoad }()

	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(*options.InitializeOptions) error {
		t.Fatal("initRuntimeWithOptionsFn must not run when handshake client fails to load")
		return nil
	}
	defer func() { initRuntimeWithOptionsFn = origInit }()

	err := initWithConfig(&HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("initWithConfig error = %v, want sentinel", err)
	}
}

func TestInitWithConfigPropagatesInitError(t *testing.T) {
	// The merged dashboard preflight would dial the fake NodeManagerPort and
	// fail (no real raylet listens in tests); stub it to pass so the sentinel
	// from initRuntimeWithOptionsFn is what propagates.
	origPre := preflightConnectFn
	preflightConnectFn = func(addr string, timeout time.Duration) error { return nil }
	defer func() { preflightConnectFn = origPre }()

	handshake := &fakeHandshakeClient{
		nodeInfo: &proto.GcsNodeInfo{ObjectStoreSocketName: "s", RayletSocketName: "r", NodeManagerPort: 1, NodeManagerAddress: "127.0.0.1"},
		jobID:    "01020304",
	}
	origLoad := loadGCSClientFn
	loadGCSClientFn = func(gcs.ClientOptions) (gcs.Client, error) { return handshake, nil }
	defer func() { loadGCSClientFn = origLoad }()

	sentinel := errors.New("base init failed")
	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(*options.InitializeOptions) error { return sentinel }
	defer func() { initRuntimeWithOptionsFn = origInit }()

	err := initWithConfig(&HeadConfig{GCSAddress: "127.0.0.1:6379", ClusterIDHex: "abc", NodeIPAddress: "127.0.0.1", SessionDir: t.TempDir()})
	if !errors.Is(err, sentinel) {
		t.Fatalf("initWithConfig error = %v, want sentinel", err)
	}
}
