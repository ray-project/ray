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
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// subscriberIDLength matches the binary format of a random SubscriberID /
// UniqueID, which is 28 (kUniqueIDSize) random bytes. See the Python
// _SubscriberBase for the reference.
const subscriberIDLength = 28

// reporterPrefix is the key id prefix for the node resource usage channel.
// The key id is of the form "RAY_REPORTER:<node id hex>", matching
// python/ray/dashboard/modules/reporter/reporter_consts.py.
const reporterPrefix = "RAY_REPORTER:"

// pollTimeout is the long-poll deadline for each Poll call. GCS flushes an
// empty reply when the connection sits idle, so a long poll keeps the
// subscription alive without missing messages. The value matches the Python
// GcsAio*Subscriber poll, which waits indefinitely: a shorter deadline makes
// the client disconnect from the GCS long poll frequently, and the GCS server
// then aborts with "server closed the stream without sending trailers".
const pollTimeout = 60 * time.Second

// NewRandomSubscriberID generates a 28-byte random subscriber id.
func NewRandomSubscriberID() []byte {
	id := make([]byte, subscriberIDLength)
	if _, err := rand.Read(id); err != nil {
		panic(fmt.Sprintf("failed to read random: %v", err))
	}
	return id
}

// Subscriber is a GCS pubsub long-polling subscriber (aligned with the Python
// _AioSubscriber). It tracks the last processed sequence id and the publisher
// id so that messages are delivered in order and the sequence is reset on GCS
// failover.
type Subscriber struct {
	client       *GCSClient
	channel      proto.ChannelType
	subscriberID []byte

	mu                sync.Mutex
	maxProcessedSeqID int64
	publisherID       []byte
}

// NewSubscriber creates a subscriber for the given channel.
func NewSubscriber(client *GCSClient, channel proto.ChannelType, subscriberID []byte) *Subscriber {
	return &Subscriber{client: client, channel: channel, subscriberID: subscriberID}
}

// Subscribe registers a subscription for the channel (aligned with
// GcsSubscriberCommandBatch in Python). The empty SubMessage matches the
// Python `pubsub_pb2.Command(channel_type=channel, subscribe_message={})`.
func (s *Subscriber) Subscribe(ctx context.Context) error {
	req := &proto.GcsSubscriberCommandBatchRequest{
		SubscriberId: s.subscriberID,
		Commands: []*proto.Command{{
			ChannelType: s.channel,
			CommandMessageOneOf: &proto.Command_SubscribeMessage{
				SubscribeMessage: &proto.SubMessage{},
			},
		}},
	}
	_, err := s.client.PubSub().GcsSubscriberCommandBatch(s.client.withClusterID(ctx), req)
	return err
}

// Poll performs a long-polling call to fetch messages (aligned with
// GcsSubscriberPoll in Python, including sequence flow control and publisher
// id failover reset).
func (s *Subscriber) Poll(ctx context.Context, timeout time.Duration) ([]*proto.PubMessage, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.mu.Lock()
	req := &proto.GcsSubscriberPollRequest{
		SubscriberId:           s.subscriberID,
		MaxProcessedSequenceId: s.maxProcessedSeqID,
		PublisherId:            s.publisherID,
	}
	s.mu.Unlock()
	reply, err := s.client.PubSub().GcsSubscriberPoll(s.client.withClusterID(ctxTimeout), req)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A publisher id change means the GCS has failed over, so the sequence is
	// no longer comparable and must be reset (aligned with Python).
	if string(reply.PublisherId) != string(s.publisherID) {
		if s.publisherID != nil {
			log.Log.Info("publisher id changed, resetting sequence; this should only happen on GCS failover",
				"channel", s.channel.String())
		}
		s.publisherID = reply.PublisherId
		s.maxProcessedSeqID = 0
	}
	for _, m := range reply.PubMessages {
		if m.SequenceId > s.maxProcessedSeqID {
			s.maxProcessedSeqID = m.SequenceId
		}
	}
	return reply.PubMessages, nil
}

// Unsubscribe deregisters the subscription for the channel.
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	req := &proto.GcsSubscriberCommandBatchRequest{
		SubscriberId: s.subscriberID,
		Commands: []*proto.Command{{
			ChannelType: s.channel,
			CommandMessageOneOf: &proto.Command_UnsubscribeMessage{
				UnsubscribeMessage: &proto.UnsubscribeMessage{},
			},
		}},
	}
	_, err := s.client.PubSub().GcsSubscriberCommandBatch(s.client.withClusterID(ctx), req)
	return err
}

// Close deregisters the subscription. The underlying gRPC connection is owned
// by the GCSClient and closed separately.
func (s *Subscriber) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Unsubscribe(ctx)
}

// runUpdates runs the long-poll loop for a subscription: it subscribes, then
// repeatedly polls and forwards each decoded message on ch. It returns after
// the context is cancelled or after a non-transient poll error, which is
// delivered on errCh. Deadline-exceeded and unavailable errors are treated as
// timed-out polls and the loop retries (aligned with Python
// _SubscriberBase._should_terminate_polling); the bounded pollTimeout also
// refreshes the server-side connection state.
func runUpdates[T any](ctx context.Context, sub *Subscriber, ch chan<- T, errCh chan<- error, decode func(*proto.PubMessage) (T, bool)) {
	if err := sub.Subscribe(ctx); err != nil {
		errCh <- fmt.Errorf("subscribe: %w", err)
		return
	}
	for {
		msgs, err := sub.Poll(ctx, pollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !isTransientPollError(err) {
				errCh <- fmt.Errorf("poll: %w", err)
				return
			}
			continue
		}
		for _, m := range msgs {
			v, ok := decode(m)
			if !ok {
				continue
			}
			select {
			case ch <- v:
			case <-ctx.Done():
				return
			}
		}
	}
}

// isTransientPollError reports whether a poll error is transient: deadline
// exceeded (a long poll that returned no messages) and unavailable (a
// temporary connection issue) should end the current poll but keep the
// subscription loop running, aligned with the Python
// _SubscriberBase._should_terminate_polling.
func isTransientPollError(err error) bool {
	code := status.Code(err)
	return code == codes.DeadlineExceeded || code == codes.Unavailable
}

// NodeInfoSubscriber subscribes to the GCS node info channel and emits
// GcsNodeInfo updates.
type NodeInfoSubscriber struct{ sub *Subscriber }

// NewNodeInfoSubscriber creates a node info subscriber.
func NewNodeInfoSubscriber(client *GCSClient) *NodeInfoSubscriber {
	return &NodeInfoSubscriber{sub: NewSubscriber(client, proto.ChannelType_GCS_NODE_INFO_CHANNEL, NewRandomSubscriberID())}
}

// Updates starts a background goroutine that long-polls the GCS node info
// channel and emits node info updates. The returned error channel receives a
// terminal error when the subscription or a poll fails in a non-transient way.
func (s *NodeInfoSubscriber) Updates(ctx context.Context) (<-chan *proto.GcsNodeInfo, <-chan error) {
	ch := make(chan *proto.GcsNodeInfo)
	errCh := make(chan error, 1)
	go runUpdates(ctx, s.sub, ch, errCh, func(m *proto.PubMessage) (*proto.GcsNodeInfo, bool) {
		info := m.GetNodeInfoMessage()
		return info, info != nil
	})
	return ch, errCh
}

// ActorSubscriber subscribes to the GCS actor channel and emits ActorTableData
// updates.
type ActorSubscriber struct{ sub *Subscriber }

// NewActorSubscriber creates an actor subscriber.
func NewActorSubscriber(client *GCSClient) *ActorSubscriber {
	return &ActorSubscriber{sub: NewSubscriber(client, proto.ChannelType_GCS_ACTOR_CHANNEL, NewRandomSubscriberID())}
}

// Updates starts a background goroutine that long-polls the GCS actor channel
// and emits actor updates. The returned error channel receives a terminal
// error when the subscription or a poll fails in a non-transient way.
func (s *ActorSubscriber) Updates(ctx context.Context) (<-chan *proto.ActorTableData, <-chan error) {
	ch := make(chan *proto.ActorTableData)
	errCh := make(chan error, 1)
	go runUpdates(ctx, s.sub, ch, errCh, func(m *proto.PubMessage) (*proto.ActorTableData, bool) {
		actor := m.GetActorMessage()
		return actor, actor != nil
	})
	return ch, errCh
}

// ResourceUsageUpdate is a node resource usage update: the node id and the
// raw JSON payload produced by the reporter.
type ResourceUsageUpdate struct {
	NodeID string
	JSON   string
}

// ResourceUsageSubscriber subscribes to the node resource usage channel and
// emits per-node resource usage updates.
type ResourceUsageSubscriber struct{ sub *Subscriber }

// NewResourceUsageSubscriber creates a resource usage subscriber.
func NewResourceUsageSubscriber(client *GCSClient) *ResourceUsageSubscriber {
	return &ResourceUsageSubscriber{sub: NewSubscriber(client, proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL, NewRandomSubscriberID())}
}

// Updates starts a background goroutine that long-polls the node resource
// usage channel and emits updates. The returned error channel receives a
// terminal error when the subscription or a poll fails in a non-transient way.
func (s *ResourceUsageSubscriber) Updates(ctx context.Context) (<-chan *ResourceUsageUpdate, <-chan error) {
	ch := make(chan *ResourceUsageUpdate)
	errCh := make(chan error, 1)
	go runUpdates(ctx, s.sub, ch, errCh, func(m *proto.PubMessage) (*ResourceUsageUpdate, bool) {
		usage := m.GetNodeResourceUsageMessage()
		if usage == nil {
			return nil, false
		}
		return &ResourceUsageUpdate{
			NodeID: strings.TrimPrefix(string(m.KeyId), reporterPrefix),
			JSON:   usage.Json,
		}, true
	})
	return ch, errCh
}
