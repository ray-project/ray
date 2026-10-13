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

package api

import (
	"context"
	"errors"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// mockGCSClient is a package-internal test double for GCSClient.
// It implements every interface method.
type mockGCSClient struct {
	pgData  *proto.PlacementGroupTableData
	pgList  []*proto.PlacementGroupTableData
	err     error
	kvValue []byte
	closed  bool
}

var _ GCSClient = (*mockGCSClient)(nil)

func (m *mockGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, m.err
}

func (m *mockGCSClient) NextJobID(ctx context.Context) (string, error) {
	return "", m.err
}

func (m *mockGCSClient) Close() error {
	m.closed = true
	return nil
}

func (m *mockGCSClient) IsClosed() bool { return m.closed }

func (m *mockGCSClient) GetPlacementGroupInfo(ctx context.Context, id ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return m.pgData, m.err
}

func (m *mockGCSClient) GetPlacementGroupInfoByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return m.pgData, m.err
}

func (m *mockGCSClient) GetAllPlacementGroupInfo(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return m.pgList, m.err
}

func (m *mockGCSClient) GetInternalKV(ctx context.Context, ns, key string) ([]byte, error) {
	return m.kvValue, m.err
}

func (m *mockGCSClient) GetAllNodeInfo(ctx context.Context) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, m.err
}

func (m *mockGCSClient) GetAllActorInfo(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, m.err
}

// recordingFactory is a test double for GCSClientFactory that records the
// options passed to CreateClient and returns a canned client.
type recordingFactory struct {
	opts      gcs.ClientOptions
	client    GCSClient
	callCount int
}

func (f *recordingFactory) CreateClient(opts gcs.ClientOptions) (GCSClient, error) {
	f.opts = opts
	f.callCount++
	return f.client, nil
}

// TestGetGCSClientNoFactory verifies that GetGCSClient returns
// ErrGCSClientFactoryNotRegistered when no factory has been registered.
func TestGetGCSClientNoFactory(t *testing.T) {
	orig := getGCSClientFactory()
	defer RegisterGCSClientFactory(orig)
	RegisterGCSClientFactory(nil)
	_, err := GetGCSClient()
	if !errors.Is(err, ErrGCSClientFactoryNotRegistered) {
		t.Fatalf("expected ErrGCSClientFactoryNotRegistered, got %v", err)
	}
}

// TestGetGCSClientNotConnected verifies that GetGCSClient returns
// ErrGCSClientNotConnected when the Ray cluster address cannot be resolved.
func TestGetGCSClientNotConnected(t *testing.T) {
	origAddr := readRayAddressFromFile
	defer func() { readRayAddressFromFile = origAddr }()
	readRayAddressFromFile = func() string { return "" }

	orig := getGCSClientFactory()
	defer RegisterGCSClientFactory(orig)
	RegisterGCSClientFactory(&recordingFactory{client: &mockGCSClient{}})

	_, err := GetGCSClient()
	if !errors.Is(err, ErrGCSClientNotConnected) {
		t.Fatalf("expected ErrGCSClientNotConnected, got %v", err)
	}
}

// TestGetGCSClientSuccess verifies that GetGCSClient resolves the address,
// passes it through to CreateClient, and reuses the cached client on
// subsequent calls.
func TestGetGCSClientSuccess(t *testing.T) {
	const addr = "127.0.0.1:6379"

	origAddr := readRayAddressFromFile
	defer func() { readRayAddressFromFile = origAddr }()
	readRayAddressFromFile = func() string { return addr }

	orig := getGCSClientFactory()
	defer RegisterGCSClientFactory(orig)
	factory := &recordingFactory{client: &mockGCSClient{}}
	RegisterGCSClientFactory(factory)

	defer gcsClientCache.Delete(addr)

	client1, err := GetGCSClient()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory.opts.Address != addr {
		t.Fatalf("expected address %q to be passed through, got %q", addr, factory.opts.Address)
	}
	if client1 != factory.client {
		t.Fatalf("expected client from factory, got %v", client1)
	}

	client2, err := GetGCSClient()
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if client2 != client1 {
		t.Fatalf("expected cached client to be reused, got %v", client2)
	}
	if factory.callCount != 1 {
		t.Fatalf("expected CreateClient to be called once due to caching, got %d calls", factory.callCount)
	}
}
