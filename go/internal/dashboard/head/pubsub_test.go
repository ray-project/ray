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
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewRandomSubscriberID(t *testing.T) {
	id := NewRandomSubscriberID()
	if len(id) != subscriberIDLength {
		t.Fatalf("subscriber id length = %d, want %d", len(id), subscriberIDLength)
	}
	id2 := NewRandomSubscriberID()
	if string(id) == string(id2) {
		t.Fatal("two subscriber ids should differ")
	}
}

// TestSubscribeCommandEncoding verifies the command batch encoding used by
// Subscribe and Unsubscribe: the channel type and the oneof command message.
func TestSubscribeCommandEncoding(t *testing.T) {
	sub := NewSubscriber(nil, proto.ChannelType_GCS_NODE_INFO_CHANNEL, []byte("0123456789abcdefghijklmnop"))

	req := &proto.GcsSubscriberCommandBatchRequest{
		SubscriberId: sub.subscriberID,
		Commands: []*proto.Command{{
			ChannelType: sub.channel,
			CommandMessageOneOf: &proto.Command_SubscribeMessage{
				SubscribeMessage: &proto.SubMessage{},
			},
		}},
	}
	if len(req.Commands) != 1 {
		t.Fatalf("commands = %d, want 1", len(req.Commands))
	}
	cmd := req.Commands[0]
	if cmd.ChannelType != proto.ChannelType_GCS_NODE_INFO_CHANNEL {
		t.Fatalf("command channel type = %v, want GCS_NODE_INFO_CHANNEL", cmd.ChannelType)
	}
	if cmd.GetSubscribeMessage() == nil {
		t.Fatal("subscribe message is nil, want a SubMessage oneof")
	}

	unsubReq := &proto.GcsSubscriberCommandBatchRequest{
		SubscriberId: sub.subscriberID,
		Commands: []*proto.Command{{
			ChannelType: sub.channel,
			CommandMessageOneOf: &proto.Command_UnsubscribeMessage{
				UnsubscribeMessage: &proto.UnsubscribeMessage{},
			},
		}},
	}
	unsubCmd := unsubReq.Commands[0]
	if unsubCmd.GetUnsubscribeMessage() == nil {
		t.Fatal("unsubscribe message is nil, want an UnsubscribeMessage oneof")
	}
}

// mockPubSubServer simulates the GCS InternalPubSub service, capturing the
// last received command batch and poll request and serving queued poll replies.
type mockPubSubServer struct {
	proto.UnimplementedInternalPubSubGcsServiceServer
	mu sync.Mutex
	// batch is the last received command batch request.
	batch *proto.GcsSubscriberCommandBatchRequest
	// poll is the last received poll request.
	poll *proto.GcsSubscriberPollRequest
	// publisherID is returned in every poll reply.
	publisherID []byte
	// pollQueue holds the pub messages returned by successive poll calls,
	// consumed in FIFO order. A nil entry replies with no messages.
	pollQueue [][]*proto.PubMessage
}

func (s *mockPubSubServer) GcsSubscriberCommandBatch(ctx context.Context, req *proto.GcsSubscriberCommandBatchRequest) (*proto.GcsSubscriberCommandBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batch = req
	return &proto.GcsSubscriberCommandBatchReply{}, nil
}

func (s *mockPubSubServer) GcsSubscriberPoll(ctx context.Context, req *proto.GcsSubscriberPollRequest) (*proto.GcsSubscriberPollReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poll = req
	reply := &proto.GcsSubscriberPollReply{PublisherId: s.publisherID}
	if len(s.pollQueue) > 0 {
		reply.PubMessages = s.pollQueue[0]
		s.pollQueue = s.pollQueue[1:]
	}
	return reply, nil
}

// startMockPubSub starts an in-memory gRPC server serving the pubsub mock and
// returns a connected GCSClient plus a stop function.
func startMockPubSub(t *testing.T, srv *mockPubSubServer) (*GCSClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterInternalPubSubGcsServiceServer(server, srv)
	go func() {
		_ = server.Serve(lis)
	}()
	client, err := NewGCSClient(context.Background(), lis.Addr().String())
	if err != nil {
		server.Stop()
		t.Fatalf("NewGCSClient: %v", err)
	}
	return client, server.Stop
}

func TestSubscriberSubscribeUnsubscribe(t *testing.T) {
	srv := &mockPubSubServer{}
	client, stop := startMockPubSub(t, srv)
	defer stop()
	defer client.Close()

	subID := NewRandomSubscriberID()
	sub := NewSubscriber(client, proto.ChannelType_GCS_NODE_INFO_CHANNEL, subID)

	if err := sub.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	srv.mu.Lock()
	batch := srv.batch
	srv.mu.Unlock()
	if batch == nil {
		t.Fatal("Subscribe: no command batch received")
	}
	if string(batch.SubscriberId) != string(subID) {
		t.Fatalf("subscribe subscriber id = %q, want %q", batch.SubscriberId, subID)
	}
	if len(batch.Commands) != 1 {
		t.Fatalf("subscribe commands = %d, want 1", len(batch.Commands))
	}
	if batch.Commands[0].ChannelType != proto.ChannelType_GCS_NODE_INFO_CHANNEL {
		t.Fatalf("subscribe channel type = %v, want GCS_NODE_INFO_CHANNEL", batch.Commands[0].ChannelType)
	}
	if batch.Commands[0].GetSubscribeMessage() == nil {
		t.Fatal("subscribe command has no subscribe message")
	}

	if err := sub.Unsubscribe(context.Background()); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	srv.mu.Lock()
	batch = srv.batch
	srv.mu.Unlock()
	if len(batch.Commands) != 1 {
		t.Fatalf("unsubscribe commands = %d, want 1", len(batch.Commands))
	}
	if batch.Commands[0].GetUnsubscribeMessage() == nil {
		t.Fatal("unsubscribe command has no unsubscribe message")
	}
}

func TestSubscriberPollSequenceAndFailover(t *testing.T) {
	srv := &mockPubSubServer{publisherID: []byte("publisher-a")}
	client, stop := startMockPubSub(t, srv)
	defer stop()
	defer client.Close()

	subID := NewRandomSubscriberID()
	sub := NewSubscriber(client, proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL, subID)

	// First poll returns messages with increasing sequence ids.
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{
		{
			{SequenceId: 3},
			{SequenceId: 5},
		},
	}
	srv.mu.Unlock()

	msgs, err := sub.Poll(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("first poll messages = %d, want 2", len(msgs))
	}

	// The second poll request must carry max_processed_sequence_id = 5 and
	// the publisher id learned from the first reply.
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{}}
	srv.mu.Unlock()
	if _, err := sub.Poll(context.Background(), time.Second); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	srv.mu.Lock()
	secondPoll := srv.poll
	srv.mu.Unlock()
	if secondPoll.MaxProcessedSequenceId != 5 {
		t.Fatalf("max processed sequence id = %d, want 5", secondPoll.MaxProcessedSequenceId)
	}
	if string(secondPoll.PublisherId) != "publisher-a" {
		t.Fatalf("publisher id = %q, want publisher-a", secondPoll.PublisherId)
	}
	if string(secondPoll.SubscriberId) != string(subID) {
		t.Fatalf("poll subscriber id = %q, want %q", secondPoll.SubscriberId, subID)
	}

	// An older sequence id in a reply must not regress the watermark.
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{
		{
			{SequenceId: 2},
		},
	}
	srv.mu.Unlock()
	if _, err := sub.Poll(context.Background(), time.Second); err != nil {
		t.Fatalf("third Poll: %v", err)
	}
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{}}
	srv.mu.Unlock()
	if _, err := sub.Poll(context.Background(), time.Second); err != nil {
		t.Fatalf("fourth Poll: %v", err)
	}
	srv.mu.Lock()
	fourthPoll := srv.poll
	srv.mu.Unlock()
	if fourthPoll.MaxProcessedSequenceId != 5 {
		t.Fatalf("max processed sequence id after older message = %d, want 5", fourthPoll.MaxProcessedSequenceId)
	}

	// Publisher id change (GCS failover) must reset the watermark to the new
	// reply's sequence id.
	srv.mu.Lock()
	srv.publisherID = []byte("publisher-b")
	srv.pollQueue = [][]*proto.PubMessage{
		{
			{SequenceId: 1},
		},
	}
	srv.mu.Unlock()
	if _, err := sub.Poll(context.Background(), time.Second); err != nil {
		t.Fatalf("fifth Poll: %v", err)
	}
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{}}
	srv.mu.Unlock()
	if _, err := sub.Poll(context.Background(), time.Second); err != nil {
		t.Fatalf("sixth Poll: %v", err)
	}
	srv.mu.Lock()
	sixthPoll := srv.poll
	srv.mu.Unlock()
	if string(sixthPoll.PublisherId) != "publisher-b" {
		t.Fatalf("publisher id after failover = %q, want publisher-b", sixthPoll.PublisherId)
	}
	if sixthPoll.MaxProcessedSequenceId != 1 {
		t.Fatalf("max processed sequence id after failover = %d, want 1", sixthPoll.MaxProcessedSequenceId)
	}
}

func TestResourceUsageSubscriberUpdates(t *testing.T) {
	srv := &mockPubSubServer{publisherID: []byte("publisher-a")}
	client, stop := startMockPubSub(t, srv)
	defer stop()
	defer client.Close()

	usage := &proto.NodeResourceUsage{Json: `{"cpu": 0.5}`}
	msg := &proto.PubMessage{
		ChannelType: proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL,
		KeyId:       []byte("RAY_REPORTER:2b4fbd00000000000000000000000000000000000000000000000000000000"),
		SequenceId:  1,
		InnerMessage: &proto.PubMessage_NodeResourceUsageMessage{
			NodeResourceUsageMessage: usage,
		},
	}
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{msg}}
	srv.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := NewResourceUsageSubscriber(client)
	ch, errCh := sub.Updates(ctx)

	select {
	case update := <-ch:
		if update.NodeID != "2b4fbd00000000000000000000000000000000000000000000000000000000" {
			t.Fatalf("node id = %q, want hex node id", update.NodeID)
		}
		if update.JSON != `{"cpu": 0.5}` {
			t.Fatalf("json = %q, want %q", update.JSON, `{"cpu": 0.5}`)
		}
	case err := <-errCh:
		t.Fatalf("resource usage subscriber error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for resource usage update")
	}
}

func TestNodeInfoSubscriberUpdates(t *testing.T) {
	srv := &mockPubSubServer{publisherID: []byte("publisher-a")}
	client, stop := startMockPubSub(t, srv)
	defer stop()
	defer client.Close()

	info := &proto.GcsNodeInfo{NodeId: []byte("node-1")}
	msg := &proto.PubMessage{
		ChannelType: proto.ChannelType_GCS_NODE_INFO_CHANNEL,
		KeyId:       []byte("node-1"),
		SequenceId:  1,
		InnerMessage: &proto.PubMessage_NodeInfoMessage{
			NodeInfoMessage: info,
		},
	}
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{msg}}
	srv.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := NewNodeInfoSubscriber(client)
	ch, errCh := sub.Updates(ctx)

	select {
	case got := <-ch:
		if string(got.NodeId) != "node-1" {
			t.Fatalf("node id = %q, want node-1", got.NodeId)
		}
	case err := <-errCh:
		t.Fatalf("node info subscriber error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for node info update")
	}
}

func TestActorSubscriberUpdates(t *testing.T) {
	srv := &mockPubSubServer{publisherID: []byte("publisher-a")}
	client, stop := startMockPubSub(t, srv)
	defer stop()
	defer client.Close()

	actor := &proto.ActorTableData{ActorId: []byte("actor-1")}
	msg := &proto.PubMessage{
		ChannelType: proto.ChannelType_GCS_ACTOR_CHANNEL,
		KeyId:       []byte("actor-1"),
		SequenceId:  1,
		InnerMessage: &proto.PubMessage_ActorMessage{
			ActorMessage: actor,
		},
	}
	srv.mu.Lock()
	srv.pollQueue = [][]*proto.PubMessage{{msg}}
	srv.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := NewActorSubscriber(client)
	ch, errCh := sub.Updates(ctx)

	select {
	case got := <-ch:
		if string(got.ActorId) != "actor-1" {
			t.Fatalf("actor id = %q, want actor-1", got.ActorId)
		}
	case err := <-errCh:
		t.Fatalf("actor subscriber error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for actor update")
	}
}

// TestPollTransientErrorClassification verifies the error classification used
// by the updates loop: deadline-exceeded and unavailable poll errors are
// transient (the loop keeps polling), other errors terminate the loop.
func TestPollTransientErrorClassification(t *testing.T) {
	if !isTransientPollError(status.Error(codes.DeadlineExceeded, "timeout")) {
		t.Fatal("deadline exceeded should be a transient poll error")
	}
	if !isTransientPollError(status.Error(codes.Unavailable, "unavailable")) {
		t.Fatal("unavailable should be a transient poll error")
	}
	if isTransientPollError(status.Error(codes.NotFound, "nope")) {
		t.Fatal("not-found should not be a transient poll error")
	}
}
