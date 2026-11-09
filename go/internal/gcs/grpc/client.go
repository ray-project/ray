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

// Package gcsgrpc implements a minimal pure-Go GCS client used by components
// that only need internal KV reads and log publishing over gRPC, avoiding the
// cgo GCS client (and therefore the go_runtime.so plugin).
package gcsgrpc

import (
	"context"
	"fmt"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Client is a pure-Go GCS gRPC client exposing the subset used by the log
// monitor: internal KV reads (autoscaler v2 detection) and log batch
// publishing. It satisfies gcs.LogBatchPublisher.
type Client struct {
	conn      *grpc.ClientConn
	clusterID string
	kv        proto.InternalKVGcsServiceClient
	pubsub    proto.ObservabilityPubSubServiceClient
}

// NewClient dials GCS and initializes the KV and pubsub service clients.
// TLS and bearer-token authentication are resolved from the Ray environment
// (RAY_USE_TLS/RAY_TLS_*, RAY_AUTH_MODE/RAY_AUTH_TOKEN*) via
// gcs.DialOptionsFromEnv, mirroring the C++ BuildChannel and Python
// init_grpc_channel. opts.TimeoutMs, when positive, is applied as the gRPC
// ConnectTimeoutMs so the dial waits for the channel to become Ready within
// that bound.
func NewClient(ctx context.Context, opts gcs.ClientOptions) (*Client, error) {
	var dialOpts gcs.DialOptions
	if opts.DialOptions != nil {
		// Caller resolved the options once (e.g. outside a retry loop) so the
		// token/TLS files are not re-read on every attempt.
		dialOpts = *opts.DialOptions
	} else {
		var err error
		dialOpts, err = gcs.DialOptionsFromEnv()
		if err != nil {
			return nil, fmt.Errorf("resolve GCS dial options: %w", err)
		}
	}
	dialOpts.ConnectTimeoutMs = opts.TimeoutMs
	// Default to the plain-TCP dialer the head also uses: grpc-go's default
	// dialer can stall the HTTP/2 cleartext handshake against the Ray GCS
	// server (see gcs.PlainTCPDialer), and the log monitor that consumes this
	// client dials the same server on the same node. A caller that set its own
	// dialer is left untouched; a nil Dialer — unset or deliberately empty —
	// falls back to the plain-TCP dialer.
	if dialOpts.Dialer == nil {
		dialOpts.Dialer = gcs.PlainTCPDialer
	}
	conn, err := gcs.DialGCS(ctx, opts.Address, dialOpts)
	if err != nil {
		return nil, fmt.Errorf("dial GCS %s: %w", opts.Address, err)
	}
	return &Client{
		conn:      conn,
		clusterID: opts.ClusterID.Hex(),
		kv:        proto.NewInternalKVGcsServiceClient(conn),
		// The GCS server registers ObservabilityPubSubService
		// (src/ray/gcs/grpc_services.cc), the same service the Python
		// subscriber uses. InternalPubSubGcsService exists only in the proto
		// and is never registered by the Ray GCS server, so publishing
		// through it fails with Unimplemented.
		pubsub: proto.NewObservabilityPubSubServiceClient(conn),
	}, nil
}

func (c *Client) withClusterID(ctx context.Context) context.Context {
	return gcs.AppendClusterID(ctx, c.clusterID)
}

// Get reads a value from the GCS internal KV store, mapping a missing key to
// gcs.ErrKeyNotFound. The GCS server normally reports it as ray::StatusCode
// NotFound encoded in reply.Status.Code with an OK transport; a gRPC NOT_FOUND
// status is mapped the same way for servers that use the transport instead.
func (c *Client) Get(ctx context.Context, ns, key string) ([]byte, error) {
	reply, err := c.kv.InternalKVGet(c.withClusterID(ctx), &proto.InternalKVGetRequest{
		Namespace: []byte(ns),
		Key:       []byte(key),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, gcs.ErrKeyNotFound
		}
		return nil, fmt.Errorf("internal kv get %s/%s: %w", ns, key, err)
	}
	if reply.Status != nil && reply.Status.Code != 0 {
		if reply.Status.Code == gcs.StatusNotFoundCode {
			return nil, gcs.ErrKeyNotFound
		}
		return nil, fmt.Errorf("internal kv get %s/%s: status code %d: %s",
			ns, key, reply.Status.Code, reply.Status.Message)
	}
	return reply.Value, nil
}

// PublishLogBatch publishes one log batch on the RAY_LOG_CHANNEL through the
// ObservabilityPubSubService, matching the wire format of the C++
// PublisherAccessor::PublishLogs (src/ray/gcs_rpc_client/accessor.cc): channel,
// key_id and log_batch_message.
func (c *Client) PublishLogBatch(ctx context.Context, payload gcs.LogBatchPayload) error {
	req := &proto.GcsPublishRequest{
		PubMessages: []*proto.PubMessage{{
			ChannelType: proto.ChannelType_RAY_LOG_CHANNEL,
			KeyId:       []byte(payload.JobID),
			InnerMessage: &proto.PubMessage_LogBatchMessage{
				LogBatchMessage: &proto.LogBatch{
					Ip:        payload.IP,
					Pid:       payload.PID,
					JobId:     payload.JobID,
					IsError:   payload.IsError,
					Lines:     payload.Lines,
					ActorName: payload.ActorName,
					TaskName:  payload.TaskName,
				},
			},
		}},
	}
	_, err := c.pubsub.GcsPublish(c.withClusterID(ctx), req)
	if err != nil {
		return fmt.Errorf("publish log batch: %w", err)
	}
	return nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error { return c.conn.Close() }
