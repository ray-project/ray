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

// Package head implements a pure-Go client to query the Ray GCS (Global
// Control Service) over gRPC. It is the backend for the dashboard head
// process that is being migrated from Python.
package head

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// grpcMaxMessageSize matches the C++ max_grpc_message_size default
// (src/ray/common/ray_config_def.h) and the Python create_gcs_channel cap
// (python/ray/_private/gcs_utils.py), both 512 MiB. Large queries such as
// GetTaskEvents / GetAllActorInfo can exceed smaller limits.
const grpcMaxMessageSize = 512 << 20

// grpcDialConnectMs bounds the gRPC dial handshake, matching the connectMs
// default in NewGCSClient.
const grpcDialConnectMs = 10_000

// GRPCOption configures the GCS client connection.
type GRPCOption func(*grpcClientConfig)

// WithTLS configures mutual TLS using the given CA, client cert and key paths.
// When caPath is empty an insecure (plaintext) connection is used.
func WithTLS(caPath, certPath, keyPath string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.caPath = caPath
		c.certPath = certPath
		c.keyPath = keyPath
	}
}

// WithToken configures per-RPC bearer token authentication.
func WithToken(token string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.token = token
	}
}

// WithClusterID sets the cluster id sent as the ray_cluster_id metadata on
// every request. The metadata key is aligned with the C++ constants.h.
func WithClusterID(clusterIDHex string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.clusterID = clusterIDHex
	}
}

type grpcClientConfig struct {
	caPath    string
	certPath  string
	keyPath   string
	token     string
	clusterID string
	connectMs int
}

// GCSClient is a pure-Go gRPC client for the GCS service.
type GCSClient struct {
	conn       *grpc.ClientConn
	clusterID  string
	actor      proto.ActorInfoGcsServiceClient
	node       proto.NodeInfoGcsServiceClient
	worker     proto.WorkerInfoGcsServiceClient
	job        proto.JobInfoGcsServiceClient
	pg         proto.PlacementGroupInfoGcsServiceClient
	task       proto.TaskInfoGcsServiceClient
	kv         proto.InternalKVGcsServiceClient
	pubsub     proto.InternalPubSubGcsServiceClient
	runtimeEnv proto.RuntimeEnvGcsServiceClient
}

// NewGCSClient establishes a connection to the GCS at address and initializes
// all service clients. The address must be in host:port form.
func NewGCSClient(ctx context.Context, address string, opts ...GRPCOption) (*GCSClient, error) {
	cfg := &grpcClientConfig{connectMs: 10_000}
	for _, o := range opts {
		o(cfg)
	}
	dialOpts := []grpc.DialOption{
		// Dial the GCS address directly with a plain TCP dialer. grpc-go's
		// default dialer fails to complete the HTTP/2 cleartext handshake with
		// the Ray GCS server (the connection stalls in CONNECTING and RPCs fail
		// with "waiting for new LB policy update"), whereas a plain net.Dial
		// completes it. This was reproduced against a live GCS and verified in
		// isolation, so force the direct TCP dialer here.
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", address)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMessageSize),
			grpc.MaxCallSendMsgSize(grpcMaxMessageSize),
		),
	}
	if cfg.caPath != "" {
		creds, err := credentials.NewClientTLSFromFile(cfg.caPath, "")
		if err != nil {
			return nil, fmt.Errorf("load TLS creds: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	// Bearer token authentication: inject authorization header per RPC.
	if cfg.token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(tokenCred{token: cfg.token}))
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(cfg.connectMs)*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctxTimeout, address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial GCS %s: %w", address, err)
	}
	return &GCSClient{
		conn:       conn,
		clusterID:  cfg.clusterID,
		actor:      proto.NewActorInfoGcsServiceClient(conn),
		node:       proto.NewNodeInfoGcsServiceClient(conn),
		worker:     proto.NewWorkerInfoGcsServiceClient(conn),
		job:        proto.NewJobInfoGcsServiceClient(conn),
		pg:         proto.NewPlacementGroupInfoGcsServiceClient(conn),
		task:       proto.NewTaskInfoGcsServiceClient(conn),
		kv:         proto.NewInternalKVGcsServiceClient(conn),
		pubsub:     proto.NewInternalPubSubGcsServiceClient(conn),
		runtimeEnv: proto.NewRuntimeEnvGcsServiceClient(conn),
	}, nil
}

// withClusterID injects the ray_cluster_id metadata, aligned with
// src/ray/common/constants.h.
func (c *GCSClient) withClusterID(ctx context.Context) context.Context {
	if c.clusterID != "" {
		return metadata.AppendToOutgoingContext(ctx, "ray_cluster_id", c.clusterID)
	}
	return ctx
}

// GetAllActorInfo queries all actors, mirroring
// StateDataSourceClient.get_all_actor_info.
func (c *GCSClient) GetAllActorInfo(ctx context.Context, req *proto.GetAllActorInfoRequest) (*proto.GetAllActorInfoReply, error) {
	return c.actor.GetAllActorInfo(c.withClusterID(ctx), req)
}

// GetAllNodeInfo queries all nodes.
func (c *GCSClient) GetAllNodeInfo(ctx context.Context, req *proto.GetAllNodeInfoRequest) (*proto.GetAllNodeInfoReply, error) {
	return c.node.GetAllNodeInfo(c.withClusterID(ctx), req)
}

// CheckAlive probes the GCS for node liveness via the node service CheckAlive
// RPC (aligned with the Python gcs_utils / dashboard head health check). An
// empty nodeIDs slice checks every node in the cluster.
func (c *GCSClient) CheckAlive(ctx context.Context, nodeIDs []string) (*proto.CheckAliveReply, error) {
	ids := make([][]byte, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		ids = append(ids, []byte(id))
	}
	return c.node.CheckAlive(c.withClusterID(ctx), &proto.CheckAliveRequest{NodeIds: ids})
}

// NewNodeManagerClient dials the raylet at address (host:port) and returns a
// NodeManagerServiceClient for querying per-node core worker stats, aligned
// with get_raylet_stub + NodeManagerServiceStub in
// python/ray/util/state/state_manager.py. The returned conn must be closed by
// the caller.
func (c *GCSClient) NewNodeManagerClient(ctx context.Context, address string) (proto.NodeManagerServiceClient, *grpc.ClientConn, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMessageSize),
			grpc.MaxCallSendMsgSize(grpcMaxMessageSize),
		),
	}
	// The raylet listens on a plaintext gRPC port (matching init_grpc_channel
	// when RAY_USE_TLS is unset in the Python flow).
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(grpcDialConnectMs)*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctxTimeout, address, dialOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial raylet %s: %w", address, err)
	}
	return proto.NewNodeManagerServiceClient(conn), conn, nil
}

// GetAllWorkerInfo queries all workers.
func (c *GCSClient) GetAllWorkerInfo(ctx context.Context, req *proto.GetAllWorkerInfoRequest) (*proto.GetAllWorkerInfoReply, error) {
	return c.worker.GetAllWorkerInfo(c.withClusterID(ctx), req)
}

// GetAllJobInfo queries all jobs.
func (c *GCSClient) GetAllJobInfo(ctx context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	return c.job.GetAllJobInfo(c.withClusterID(ctx), req)
}

// GetAllPlacementGroup queries all placement groups.
func (c *GCSClient) GetAllPlacementGroup(ctx context.Context, req *proto.GetAllPlacementGroupRequest) (*proto.GetAllPlacementGroupReply, error) {
	return c.pg.GetAllPlacementGroup(c.withClusterID(ctx), req)
}

// GetTaskEvents queries task events for the given job id.
func (c *GCSClient) GetTaskEvents(ctx context.Context, req *proto.GetTaskEventsRequest) (*proto.GetTaskEventsReply, error) {
	return c.task.GetTaskEvents(c.withClusterID(ctx), req)
}

// PubSub returns the GCS internal pubsub service client, used by the
// long-polling subscribers (go/internal/dashboard/head/pubsub.go).
func (c *GCSClient) PubSub() proto.InternalPubSubGcsServiceClient {
	return c.pubsub
}

// RuntimeEnv returns the GCS runtime env service client, used to pin
// runtime_env package URIs.
func (c *GCSClient) RuntimeEnv() proto.RuntimeEnvGcsServiceClient {
	return c.runtimeEnv
}

// InternalKVGet reads a value from the GCS internal KV store.
func (c *GCSClient) InternalKVGet(ctx context.Context, ns, key string) (*proto.InternalKVGetReply, error) {
	req := &proto.InternalKVGetRequest{Namespace: []byte(ns), Key: []byte(key)}
	return c.kv.InternalKVGet(c.withClusterID(ctx), req)
}

// InternalKVPut writes a value to the GCS internal KV store. It returns
// whether the entry was newly added.
func (c *GCSClient) InternalKVPut(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	req := &proto.InternalKVPutRequest{
		Namespace: []byte(ns),
		Key:       []byte(key),
		Value:     value,
		Overwrite: overwrite,
	}
	reply, err := c.kv.InternalKVPut(c.withClusterID(ctx), req)
	if err != nil {
		return false, err
	}
	return reply.Added, nil
}

// InternalKVKeys lists keys under a namespace with the given prefix.
func (c *GCSClient) InternalKVKeys(ctx context.Context, ns, prefix string) ([]string, error) {
	req := &proto.InternalKVKeysRequest{Namespace: []byte(ns), Prefix: []byte(prefix)}
	reply, err := c.kv.InternalKVKeys(c.withClusterID(ctx), req)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(reply.Results))
	for _, r := range reply.Results {
		keys = append(keys, string(r))
	}
	return keys, nil
}

// InternalKVDel deletes a key (or all keys under a prefix) and returns the
// number of deleted entries.
func (c *GCSClient) InternalKVDel(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	req := &proto.InternalKVDelRequest{
		Namespace:   []byte(ns),
		Key:         []byte(key),
		DelByPrefix: delByPrefix,
	}
	reply, err := c.kv.InternalKVDel(c.withClusterID(ctx), req)
	if err != nil {
		return 0, err
	}
	return int(reply.DeletedNum), nil
}

// tokenCred implements credentials.PerRPCCredentials for bearer token auth.
type tokenCred struct{ token string }

func (t tokenCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}

func (t tokenCred) RequireTransportSecurity() bool { return false }

// Close closes the underlying gRPC connection.
func (c *GCSClient) Close() error { return c.conn.Close() }
