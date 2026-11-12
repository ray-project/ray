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
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// grpcDialer is the plain-TCP dialer the head hands to gcs.DialGCS. It is a
// package variable so tests can observe that the built connection dials
// through it; see gcs.PlainTCPDialer for why grpc-go's default dialer cannot
// be used.
var grpcDialer = gcs.PlainTCPDialer

// GRPCOption configures the GCS client connection.
type GRPCOption func(*grpcClientConfig)

// WithTLS configures mutual TLS using the given CA, client cert and key paths.
// Empty paths leave the corresponding previously resolved value untouched, so
// an explicit option only overrides what the environment contributed when it
// is non-empty. Use no WithTLS option for a plaintext connection.
func WithTLS(caPath, certPath, keyPath string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.opts = c.opts.Override(gcs.DialOptions{
			CAFile:   caPath,
			CertFile: certPath,
			KeyFile:  keyPath,
		})
	}
}

// WithToken configures per-RPC bearer token authentication. An empty token
// leaves a previously resolved token (for example one assembled from the
// environment by GCSClientOptionsFromEnv) in place.
func WithToken(token string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.opts = c.opts.Override(gcs.DialOptions{Token: token})
	}
}

// WithClusterID sets the cluster id sent as the ray_cluster_id metadata on
// every request. The metadata key is aligned with the C++ constants.h.
func WithClusterID(clusterIDHex string) GRPCOption {
	return func(c *grpcClientConfig) {
		c.clusterID = clusterIDHex
	}
}

// Environment variables gating Ray token authentication, aligned with the
// RAY_* env reads of src/ray/rpc/authentication/ (RayConfig) and
// k8s_constants.h kRaySaTokenPath.
const (
	authModeEnvVar      = "RAY_AUTH_MODE"
	authTokenEnvVar     = "RAY_AUTH_TOKEN"
	authTokenPathEnvVar = "RAY_AUTH_TOKEN_PATH"
	k8sTokenAuthEnvVar  = "RAY_ENABLE_K8S_TOKEN_AUTH"
)

// GCSClientOptionsFromEnv assembles the GCS client options from the process
// environment, mirroring the authentication machinery every Ray client
// shares (src/ray/rpc/grpc_client.cc BuildChannel +
// authentication_token_loader.cc): token auth is active only when
// RAY_AUTH_MODE is "token" (case-insensitive) — the posture ray.init gives
// every child process by default since enable_token_auth_by_default. When
// active, the token must be attached, otherwise GCS rejects every RPC as
// Unauthenticated (the dashboard head could then never publish its address).
// The token value follows the C++ AuthenticationTokenLoader source
// precedence: RAY_AUTH_TOKEN, the RAY_AUTH_TOKEN_PATH file, the Ray
// service-account token when RAY_ENABLE_K8S_TOKEN_AUTH is on, then the
// default ~/.ray/auth_token file. Unlike the C++ loader (RAY_CHECK), a
// configured-but-unreadable token file falls through to the next source: the
// client never sends a wrong token, and the head degrades on GCS rejections
// instead of dying.
//
// This is the head's authentication decision point: NewGCSClient takes only
// explicit options, so the environment is consulted exactly once here per
// client build.
func GCSClientOptionsFromEnv(clusterIDHex string) []GRPCOption {
	opts := []GRPCOption{WithClusterID(clusterIDHex)}
	if !strings.EqualFold(os.Getenv(authModeEnvVar), "token") {
		return opts
	}
	if token := authTokenFromEnv(); token != "" {
		opts = append(opts, WithToken(token))
	}
	return opts
}

// authTokenFromEnv loads the authentication token with the C++
// AuthenticationTokenLoader source precedence (see GCSClientOptionsFromEnv).
// File reads go through gcs.ReadTokenFile, which matches the head's
// readFirstLine contract (first non-empty trimmed line); a read error counts
// as no token so resolution falls through to the next source.
func authTokenFromEnv() string {
	if tok := strings.TrimSpace(os.Getenv(authTokenEnvVar)); tok != "" {
		return tok
	}
	if path := os.Getenv(authTokenPathEnvVar); path != "" {
		if tok, err := gcs.ReadTokenFile(path); err == nil && tok != "" {
			return tok
		}
	}
	// RayConfig parses bool envs as lower(value) in {"true", "1"}.
	if v := os.Getenv(k8sTokenAuthEnvVar); v == "1" || strings.EqualFold(v, "true") {
		if tok, err := gcs.ReadTokenFile(gcs.RayServiceAccountTokenPath); err == nil && tok != "" {
			return tok
		}
	}
	tok, _ := gcs.ReadTokenFile(defaultAuthTokenPath())
	return tok
}

// defaultAuthTokenPath returns ~/.ray/auth_token, aligned with
// AuthenticationTokenLoader::GetDefaultTokenPath: HOME, falling back to the
// user database when HOME is unset. The one deviation is logging, not path
// resolution: the C++ loader warns ("Cannot determine home directory for
// token storage") when no home resolves, while this code stays silent.
// Either way an empty result makes the token read find no token.
func defaultAuthTokenPath() string {
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := user.Current(); err == nil {
			home = u.HomeDir
		}
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, gcs.DefaultAuthTokenPath)
}

type grpcClientConfig struct {
	opts      gcs.DialOptions
	clusterID string
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
	// The GCS server registers pubsub RPCs on two services, and a channel is
	// served by exactly one of them: ControlPlanePubSubGcsService for the
	// node/actor/job channels, ObservabilityPubSubService for the log, error
	// and node-resource-usage channels (python/ray/_private/gcs_pubsub.py
	// selects between them the same way). InternalPubSubGcsService exists only
	// in the proto and is never registered, so subscribing through it fails
	// with Unimplemented.
	controlPlanePubSub  proto.ControlPlanePubSubGcsServiceClient
	observabilityPubSub proto.ObservabilityPubSubServiceClient
	runtimeEnv          proto.RuntimeEnvGcsServiceClient
}

// NewGCSClient establishes a connection to the GCS at address and initializes
// all service clients. The address must be in host:port form.
//
// The dial is non-blocking: gcs.DialGCS starts the connection attempt and
// returns without waiting for the channel to become Ready. TLS and bearer-token
// authentication are explicit only; callers assemble them from the environment
// with GCSClientOptionsFromEnv.
func NewGCSClient(ctx context.Context, address string, opts ...GRPCOption) (*GCSClient, error) {
	cfg := &grpcClientConfig{}
	for _, o := range opts {
		o(cfg)
	}
	dialOpts := cfg.opts
	// Zero keeps the head's dial non-blocking (never a bounded wait on Ready).
	dialOpts.ConnectTimeoutMs = 0
	dialOpts.Dialer = grpcDialer
	conn, err := gcs.DialGCS(ctx, address, dialOpts)
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
		controlPlanePubSub:  proto.NewControlPlanePubSubGcsServiceClient(conn),
		observabilityPubSub: proto.NewObservabilityPubSubServiceClient(conn),
		runtimeEnv: proto.NewRuntimeEnvGcsServiceClient(conn),
	}, nil
}

// withClusterID injects the ray_cluster_id metadata, aligned with
// src/ray/common/constants.h.
func (c *GCSClient) withClusterID(ctx context.Context) context.Context {
	return gcs.AppendClusterID(ctx, c.clusterID)
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
//
// Every client channel presents the cluster's credentials resolved from the
// environment, as the C++ BuildChannel does: under RAY_AUTH_MODE=token the
// raylet rejects unauthenticated requests. The resolution runs per dial so a
// rotated projected token is picked up by the next client.
func (c *GCSClient) NewNodeManagerClient(ctx context.Context, address string) (proto.NodeManagerServiceClient, *grpc.ClientConn, error) {
	// The raylet listens on a plaintext gRPC port (matching init_grpc_channel
	// when RAY_USE_TLS is unset in the Python flow). DialGCS applies the shared
	// 512 MiB message cap; a zero ConnectTimeoutMs keeps the dial non-blocking,
	// and the default dialer stays in place because this dials a raylet, not
	// the GCS.
	dialOpts, err := gcs.DialOptionsFromEnv()
	if err != nil {
		return nil, nil, fmt.Errorf("dial raylet %s: %w", address, err)
	}
	conn, err := gcs.DialGCS(ctx, address, dialOpts)
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

// pubSubClient is the subset of the two pubsub services the subscribers use.
// Both services expose these RPCs with identical signatures; only the service
// a channel is registered on differs.
type pubSubClient interface {
	GcsSubscriberPoll(ctx context.Context, in *proto.GcsSubscriberPollRequest, opts ...grpc.CallOption) (*proto.GcsSubscriberPollReply, error)
	GcsSubscriberCommandBatch(ctx context.Context, in *proto.GcsSubscriberCommandBatchRequest, opts ...grpc.CallOption) (*proto.GcsSubscriberCommandBatchReply, error)
}

// observabilityPubSubChannels mirrors Python's _OBSERVABILITY_PUBSUB_CHANNELS:
// the channels served by ObservabilityPubSubService. Everything else is served
// by ControlPlanePubSubGcsService.
var observabilityPubSubChannels = map[proto.ChannelType]bool{
	proto.ChannelType_RAY_ERROR_INFO_CHANNEL:          true,
	proto.ChannelType_RAY_LOG_CHANNEL:                 true,
	proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL: true,
}

// PubSubFor returns the pubsub service client that serves channel. Subscribing
// on the wrong one fails with Unimplemented.
func (c *GCSClient) PubSubFor(channel proto.ChannelType) pubSubClient {
	if observabilityPubSubChannels[channel] {
		return c.observabilityPubSub
	}
	return c.controlPlanePubSub
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

// Close closes the underlying gRPC connection.
func (c *GCSClient) Close() error { return c.conn.Close() }
