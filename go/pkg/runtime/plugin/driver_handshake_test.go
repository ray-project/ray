package plugin

import (
	"context"
	"errors"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/proto"
)

// fakeHandshakeClient implements GCSHandshakeClient for tests.
type fakeHandshakeClient struct {
	nodeInfo     *proto.GcsNodeInfo
	nodeErr      error
	jobID        ids.JobID
	jobErr       error
	nextJobCalls int
}

func (f *fakeHandshakeClient) GetNodeToConnect(ctx context.Context, ip string) (*proto.GcsNodeInfo, error) {
	return f.nodeInfo, f.nodeErr
}

func (f *fakeHandshakeClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	f.nextJobCalls++
	return f.jobID, f.jobErr
}

func nodeInfo() *proto.GcsNodeInfo {
	return &proto.GcsNodeInfo{
		NodeManagerAddress:    "10.0.0.5",
		RayletSocketName:      "/tmp/ray/session/raylet.sock",
		ObjectStoreSocketName: "/tmp/ray/session/store.sock",
		NodeManagerPort:       12345,
	}
}

func TestDriverHandshake_FillsMissingFields(t *testing.T) {
	client := &fakeHandshakeClient{nodeInfo: nodeInfo(), jobID: mustJobID(t, "01020304")}
	opts := &options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network:    options.NetworkOptions{GcsAddress: "127.0.0.1:6379"},
	}

	if err := DriverHandshake(context.Background(), client, opts); err != nil {
		t.Fatalf("DriverHandshake() error = %v", err)
	}
	if opts.Runtime.StoreSocket != "/tmp/ray/session/store.sock" {
		t.Errorf("StoreSocket = %q, want store.sock", opts.Runtime.StoreSocket)
	}
	if opts.Runtime.RayletSocket != "/tmp/ray/session/raylet.sock" {
		t.Errorf("RayletSocket = %q, want raylet.sock", opts.Runtime.RayletSocket)
	}
	if opts.Network.NodeManagerPort != 12345 {
		t.Errorf("NodeManagerPort = %d, want 12345", opts.Network.NodeManagerPort)
	}
	if opts.Network.NodeIPAddress != "10.0.0.5" {
		t.Errorf("NodeIPAddress = %q, want 10.0.0.5", opts.Network.NodeIPAddress)
	}
	if opts.Job.JobID != "01020304" {
		t.Errorf("JobID = %q, want 01020304", opts.Job.JobID)
	}
	if client.nextJobCalls != 1 {
		t.Errorf("NextJobID called %d times, want 1", client.nextJobCalls)
	}
}

func TestDriverHandshake_PreservesExplicitConfig(t *testing.T) {
	client := &fakeHandshakeClient{nodeInfo: nodeInfo(), jobID: mustJobID(t, "01020304")}
	// NodeIPAddress left empty so the node-info fetch still triggers; the
	// explicit socket/port must win and the IP is filled from GCS.
	opts := &options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network: options.NetworkOptions{
			GcsAddress:      "127.0.0.1:6379",
			NodeManagerPort: 2222,
		},
		Runtime: options.RuntimeOptions{
			StoreSocket:  "/custom/store.sock",
			RayletSocket: "/custom/raylet.sock",
		},
		Job: options.JobOptions{JobID: "aabbccdd"},
	}

	if err := DriverHandshake(context.Background(), client, opts); err != nil {
		t.Fatalf("DriverHandshake() error = %v", err)
	}
	// Explicit socket/port config wins.
	if opts.Runtime.StoreSocket != "/custom/store.sock" {
		t.Errorf("StoreSocket = %q, want explicit /custom/store.sock", opts.Runtime.StoreSocket)
	}
	if opts.Runtime.RayletSocket != "/custom/raylet.sock" {
		t.Errorf("RayletSocket = %q, want explicit /custom/raylet.sock", opts.Runtime.RayletSocket)
	}
	if opts.Network.NodeManagerPort != 2222 {
		t.Errorf("NodeManagerPort = %d, want explicit 2222", opts.Network.NodeManagerPort)
	}
	// NodeIPAddress is authoritative from GCS when fetched.
	if opts.Network.NodeIPAddress != "10.0.0.5" {
		t.Errorf("NodeIPAddress = %q, want GCS 10.0.0.5 (authoritative)", opts.Network.NodeIPAddress)
	}
	// Explicit JobID wins; no allocation.
	if opts.Job.JobID != "aabbccdd" {
		t.Errorf("JobID = %q, want explicit aabbccdd", opts.Job.JobID)
	}
	if client.nextJobCalls != 0 {
		t.Errorf("NextJobID called %d times, want 0 (JobID already set)", client.nextJobCalls)
	}
}

func TestDriverHandshake_AllFieldsSetSkipsNodeFetch(t *testing.T) {
	client := &fakeHandshakeClient{nodeInfo: nodeInfo(), jobID: mustJobID(t, "01020304")}
	opts := &options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network: options.NetworkOptions{
			GcsAddress:      "127.0.0.1:6379",
			NodeManagerPort: 2222,
			NodeIPAddress:   "192.168.1.1",
		},
		Runtime: options.RuntimeOptions{
			StoreSocket:  "/custom/store.sock",
			RayletSocket: "/custom/raylet.sock",
		},
		Job: options.JobOptions{JobID: "aabbccdd"},
	}

	// Every connection field is set, so no GCS node-info fetch happens and the
	// caller's values are all preserved (matches the plugin Initialize trigger).
	if err := DriverHandshake(context.Background(), client, opts); err != nil {
		t.Fatalf("DriverHandshake() error = %v", err)
	}
	if opts.Network.NodeIPAddress != "192.168.1.1" {
		t.Errorf("NodeIPAddress = %q, want caller 192.168.1.1 (fetch skipped)", opts.Network.NodeIPAddress)
	}
	if opts.Network.NodeManagerPort != 2222 {
		t.Errorf("NodeManagerPort = %d, want 2222", opts.Network.NodeManagerPort)
	}
}

func TestDriverHandshake_NodeFetchErrorTolerated(t *testing.T) {
	client := &fakeHandshakeClient{
		nodeErr: errors.New("gcs down"),
		jobID:   mustJobID(t, "01020304"),
	}
	opts := &options.InitializeOptions{WorkerType: options.WorkerTypeDriver}

	// A node-info fetch failure must not fail the handshake (base.Initialize
	// validation is relaxed for Driver mode); JobID allocation still happens.
	if err := DriverHandshake(context.Background(), client, opts); err != nil {
		t.Fatalf("DriverHandshake() error = %v, want nil despite node fetch error", err)
	}
	if opts.Job.JobID != "01020304" {
		t.Errorf("JobID = %q, want 01020304 allocated despite node fetch error", opts.Job.JobID)
	}
}

func TestDriverHandshake_NextJobIDError(t *testing.T) {
	client := &fakeHandshakeClient{
		nodeInfo: nodeInfo(),
		jobErr:   errors.New("no more ids"),
	}
	opts := &options.InitializeOptions{WorkerType: options.WorkerTypeDriver}

	if err := DriverHandshake(context.Background(), client, opts); err == nil {
		t.Fatal("DriverHandshake() = nil error, want error when NextJobID fails")
	}
}

func TestDriverHandshake_NilArgs(t *testing.T) {
	opts := &options.InitializeOptions{}
	if err := DriverHandshake(context.Background(), nil, opts); err == nil {
		t.Fatal("DriverHandshake(nil client) = nil error, want error")
	}
	if err := DriverHandshake(context.Background(), &fakeHandshakeClient{}, nil); err == nil {
		t.Fatal("DriverHandshake(nil opts) = nil error, want error")
	}
}

func mustJobID(t *testing.T, hex string) ids.JobID {
	t.Helper()
	id, err := ids.JobIDFromHex(hex)
	if err != nil {
		t.Fatalf("JobIDFromHex(%q): %v", hex, err)
	}
	return id
}
