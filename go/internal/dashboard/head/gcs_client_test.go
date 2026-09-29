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
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockInternalKVServer simulates the GCS InternalKV service, capturing the
// ray_cluster_id metadata and KV state so tests can assert on them.
type mockInternalKVServer struct {
	proto.UnimplementedInternalKVGcsServiceServer
	mu          sync.Mutex
	gotMetadata metadata.MD
	store       map[string][]byte
}

func newMockInternalKVServer() *mockInternalKVServer {
	return &mockInternalKVServer{store: make(map[string][]byte)}
}

func (s *mockInternalKVServer) nsKey(namespace, key []byte) string {
	return string(namespace) + "\x00" + string(key)
}

func (s *mockInternalKVServer) capture(ctx context.Context) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.mu.Lock()
		s.gotMetadata = md.Copy()
		s.mu.Unlock()
	}
}

func (s *mockInternalKVServer) gotClusterID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if vals := s.gotMetadata.Get("ray_cluster_id"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (s *mockInternalKVServer) gotToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if vals := s.gotMetadata.Get("authorization"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (s *mockInternalKVServer) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	s.capture(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.store[s.nsKey(req.Namespace, req.Key)]
	return &proto.InternalKVGetReply{Value: value}, nil
}

func (s *mockInternalKVServer) InternalKVPut(ctx context.Context, req *proto.InternalKVPutRequest) (*proto.InternalKVPutReply, error) {
	s.capture(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.nsKey(req.Namespace, req.Key)
	_, existed := s.store[key]
	if existed && !req.Overwrite {
		return &proto.InternalKVPutReply{Added: false}, nil
	}
	s.store[key] = req.Value
	return &proto.InternalKVPutReply{Added: true}, nil
}

func (s *mockInternalKVServer) InternalKVKeys(ctx context.Context, req *proto.InternalKVKeysRequest) (*proto.InternalKVKeysReply, error) {
	s.capture(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(req.Prefix)
	results := make([][]byte, 0)
	for k := range s.store {
		if strings.HasPrefix(k, string(req.Namespace)+"\x00"+prefix) {
			results = append(results, []byte(strings.TrimPrefix(k, string(req.Namespace)+"\x00")))
		}
	}
	return &proto.InternalKVKeysReply{Results: results}, nil
}

func (s *mockInternalKVServer) InternalKVDel(ctx context.Context, req *proto.InternalKVDelRequest) (*proto.InternalKVDelReply, error) {
	s.capture(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := int32(0)
	if req.DelByPrefix {
		prefix := string(req.Namespace) + "\x00" + string(req.Key)
		for k := range s.store {
			if strings.HasPrefix(k, prefix) {
				delete(s.store, k)
				deleted++
			}
		}
	} else {
		if _, ok := s.store[s.nsKey(req.Namespace, req.Key)]; ok {
			delete(s.store, s.nsKey(req.Namespace, req.Key))
			deleted++
		}
	}
	return &proto.InternalKVDelReply{DeletedNum: deleted}, nil
}

// startMockGCS starts an in-memory gRPC server serving mockInternalKVServer
// and returns the listen address and a stop function.
func startMockGCS(t *testing.T, srv *mockInternalKVServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(server, srv)
	go func() {
		_ = server.Serve(lis)
	}()
	return lis.Addr().String(), server.Stop
}

func TestGCSClientClusterIDMetadata(t *testing.T) {
	srv := newMockInternalKVServer()
	addr, stop := startMockGCS(t, srv)
	defer stop()

	client, err := NewGCSClient(context.Background(), addr, WithClusterID("0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	defer client.Close()

	reply, err := client.InternalKVGet(context.Background(), "ns", "k1")
	if err != nil {
		t.Fatalf("InternalKVGet: %v", err)
	}
	if reply == nil {
		t.Fatal("InternalKVGet returned nil reply")
	}
	if got := srv.gotClusterID(); got != "0123456789abcdef" {
		t.Fatalf("cluster id metadata = %q, want %q", got, "0123456789abcdef")
	}
}

func TestGCSClientTokenMetadata(t *testing.T) {
	srv := newMockInternalKVServer()
	addr, stop := startMockGCS(t, srv)
	defer stop()

	client, err := NewGCSClient(context.Background(), addr, WithToken("secret-token"))
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	defer client.Close()

	if _, err := client.InternalKVGet(context.Background(), "ns", "k1"); err != nil {
		t.Fatalf("InternalKVGet: %v", err)
	}
	if got := srv.gotToken(); got != "Bearer secret-token" {
		t.Fatalf("authorization metadata = %q, want %q", got, "Bearer secret-token")
	}
}

func TestGCSClientNoClusterIDWhenNotConfigured(t *testing.T) {
	srv := newMockInternalKVServer()
	addr, stop := startMockGCS(t, srv)
	defer stop()

	client, err := NewGCSClient(context.Background(), addr)
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	defer client.Close()

	if _, err := client.InternalKVGet(context.Background(), "ns", "k1"); err != nil {
		t.Fatalf("InternalKVGet: %v", err)
	}
	if got := srv.gotClusterID(); got != "" {
		t.Fatalf("cluster id metadata = %q, want empty", got)
	}
}

func TestGCSClientInternalKVOperations(t *testing.T) {
	srv := newMockInternalKVServer()
	addr, stop := startMockGCS(t, srv)
	defer stop()

	client, err := NewGCSClient(context.Background(), addr, WithClusterID("0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Put a value and verify the added flag.
	added, err := client.InternalKVPut(ctx, "ns", "keyA", []byte("valueA"), false)
	if err != nil {
		t.Fatalf("InternalKVPut: %v", err)
	}
	if !added {
		t.Fatal("InternalKVPut added = false, want true")
	}

	// Overwrite=false on an existing key should not replace the value.
	added, err = client.InternalKVPut(ctx, "ns", "keyA", []byte("overwrite-blocked"), false)
	if err != nil {
		t.Fatalf("InternalKVPut (no overwrite): %v", err)
	}
	if added {
		t.Fatal("InternalKVPut (no overwrite) added = true, want false")
	}
	reply, err := client.InternalKVGet(ctx, "ns", "keyA")
	if err != nil {
		t.Fatalf("InternalKVGet: %v", err)
	}
	if got := string(reply.Value); got != "valueA" {
		t.Fatalf("InternalKVGet value = %q, want %q", got, "valueA")
	}

	// Overwrite=true should replace.
	added, err = client.InternalKVPut(ctx, "ns", "keyA", []byte("valueB"), true)
	if err != nil {
		t.Fatalf("InternalKVPut (overwrite): %v", err)
	}
	if !added {
		t.Fatal("InternalKVPut (overwrite) added = false, want true")
	}

	// Keys with prefix.
	keys, err := client.InternalKVKeys(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("InternalKVKeys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "keyA" {
		t.Fatalf("InternalKVKeys = %v, want [keyA]", keys)
	}
	empty, err := client.InternalKVKeys(ctx, "ns", "nope")
	if err != nil {
		t.Fatalf("InternalKVKeys (no match): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("InternalKVKeys (no match) = %v, want empty", empty)
	}

	// Delete by key.
	deleted, err := client.InternalKVDel(ctx, "ns", "keyA", false)
	if err != nil {
		t.Fatalf("InternalKVDel: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("InternalKVDel deleted = %d, want 1", deleted)
	}

	// Delete by prefix.
	_, _ = client.InternalKVPut(ctx, "ns", "prefix1", []byte("v1"), false)
	_, _ = client.InternalKVPut(ctx, "ns", "prefix2", []byte("v2"), false)
	deleted, err = client.InternalKVDel(ctx, "ns", "prefix", true)
	if err != nil {
		t.Fatalf("InternalKVDel (by prefix): %v", err)
	}
	if deleted != 2 {
		t.Fatalf("InternalKVDel (by prefix) deleted = %d, want 2", deleted)
	}
}
