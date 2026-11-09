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

package gcsgrpc

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedObservabilityPubSubServiceServer
	kvValue    []byte
	kvNotFound bool
	kvErr      error
	gotPub     *proto.GcsPublishRequest
	gotMD      metadata.MD
}

func (f *fakeGCS) InternalKVGet(_ context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	if f.kvErr != nil {
		return nil, f.kvErr
	}
	if f.kvNotFound {
		// Match the real GCS server wire behavior (GCS_RPC_SEND_REPLY in
		// src/ray/gcs/grpc_service_interfaces.h): transport OK with the error
		// encoded in reply.Status.Code (ray::StatusCode::NotFound == 17).
		return &proto.InternalKVGetReply{
			Status: &proto.GcsStatus{Code: 17, Message: "Failed to find the key"},
		}, nil
	}
	return &proto.InternalKVGetReply{Status: &proto.GcsStatus{Code: 0}, Value: f.kvValue}, nil
}

func (f *fakeGCS) GcsPublish(ctx context.Context, req *proto.GcsPublishRequest) (*proto.GcsPublishReply, error) {
	f.gotPub = req
	f.gotMD, _ = metadata.FromIncomingContext(ctx)
	return &proto.GcsPublishReply{Status: &proto.GcsStatus{Code: 0}}, nil
}

func startMockGCS(t *testing.T, f *fakeGCS) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(server, f)
	proto.RegisterObservabilityPubSubServiceServer(server, f)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

func newTestClient(t *testing.T, f *fakeGCS) *Client {
	t.Helper()
	addr := startMockGCS(t, f)
	cc, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return &Client{conn: cc, kv: proto.NewInternalKVGcsServiceClient(cc), pubsub: proto.NewObservabilityPubSubServiceClient(cc)}
}

func TestGetNotFound(t *testing.T) {
	f := &fakeGCS{kvNotFound: true}
	c := newTestClient(t, f)
	_, err := c.Get(context.Background(), "__autoscaler", "k")
	if !errors.Is(err, gcs.ErrKeyNotFound) {
		t.Fatalf("Get() error = %v, want gcs.ErrKeyNotFound", err)
	}
}

func TestGetNotFoundTransportStatus(t *testing.T) {
	f := &fakeGCS{kvErr: status.Error(codes.NotFound, "no such key")}
	c := newTestClient(t, f)
	_, err := c.Get(context.Background(), "__autoscaler", "k")
	if !errors.Is(err, gcs.ErrKeyNotFound) {
		t.Fatalf("Get() error = %v, want gcs.ErrKeyNotFound", err)
	}
}

func TestGetValue(t *testing.T) {
	f := &fakeGCS{kvValue: []byte("1")}
	c := newTestClient(t, f)
	got, err := c.Get(context.Background(), "__autoscaler", "k")
	if err != nil || string(got) != "1" {
		t.Fatalf("Get() = %q, %v; want %q, nil", got, err, "1")
	}
}

func TestNewClientUsesPreResolvedDialOptions(t *testing.T) {
	f := &fakeGCS{kvValue: []byte("1")}
	addr := startMockGCS(t, f)
	pre := gcs.DialOptions{ConnectTimeoutMs: 5000}
	// RAY_USE_TLS without its certificate variables makes DialOptionsFromEnv
	// fail, and the absurd auth mode is never a valid setting: either being
	// consulted would surface, so reaching the server proves the pre-resolved
	// options were used verbatim.
	t.Setenv("RAY_USE_TLS", "1")
	t.Setenv("RAY_AUTH_MODE", "definitely-not-a-real-auth-mode")
	c, err := NewClient(context.Background(), gcs.ClientOptions{
		Address:     addr,
		TimeoutMs:   5000,
		DialOptions: &pre,
	})
	if err != nil {
		t.Fatalf("NewClient() with pre-resolved DialOptions error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	got, err := c.Get(context.Background(), "__autoscaler", "k")
	if err != nil || string(got) != "1" {
		t.Fatalf("Get() = %q, %v; want %q, nil", got, err, "1")
	}
}

func TestPublishLogBatchWireFormat(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)
	payload := gcs.LogBatchPayload{
		IP: "1.2.3.4", PID: "123", JobID: "01000000",
		IsError: true, Lines: []string{"l1"}, ActorName: "a", TaskName: "t",
	}
	if err := c.PublishLogBatch(context.Background(), payload); err != nil {
		t.Fatalf("PublishLogBatch() error = %v", err)
	}
	if len(f.gotPub.PubMessages) != 1 {
		t.Fatalf("PubMessages = %d, want 1 message", len(f.gotPub.PubMessages))
	}
	msg := f.gotPub.PubMessages[0]
	if msg.ChannelType != proto.ChannelType_RAY_LOG_CHANNEL {
		t.Fatalf("channel = %v, want RAY_LOG_CHANNEL", msg.ChannelType)
	}
	if string(msg.KeyId) != "01000000" {
		t.Fatalf("key_id = %q, want 01000000", msg.KeyId)
	}
	lb := msg.GetLogBatchMessage()
	if lb.Ip != "1.2.3.4" || lb.Pid != "123" || lb.JobId != "01000000" || !lb.IsError ||
		len(lb.Lines) != 1 || lb.Lines[0] != "l1" || lb.ActorName != "a" || lb.TaskName != "t" {
		t.Fatalf("LogBatch = %#v, want all payload fields", lb)
	}
}

func TestPublishLogBatchSendsClusterIDMetadata(t *testing.T) {
	f := &fakeGCS{}
	addr := startMockGCS(t, f)
	cc, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	c := &Client{
		conn:      cc,
		clusterID: "abc123",
		kv:        proto.NewInternalKVGcsServiceClient(cc),
		pubsub:    proto.NewObservabilityPubSubServiceClient(cc),
	}

	if err := c.PublishLogBatch(context.Background(), gcs.LogBatchPayload{JobID: "01000000", Lines: []string{"l1"}}); err != nil {
		t.Fatalf("PublishLogBatch() error = %v", err)
	}
	if got := f.gotMD.Get("ray_cluster_id"); len(got) != 1 || got[0] != "abc123" {
		t.Fatalf("ray_cluster_id metadata = %v, want [abc123]", got)
	}
}
