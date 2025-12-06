// Copyright 2021 The Ray Authors.
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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/grpc_util.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "ray/util/network_util.h"
#include "src/ray/protobuf/pubsub.grpc.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {
namespace pubsub {

// Implements SubscriberService for handling subscriber polling.
class SubscriberServiceImpl final : public rpc::SubscriberService::CallbackService {
 public:
  explicit SubscriberServiceImpl(std::unique_ptr<Publisher> publisher)
      : publisher_(std::move(publisher)) {}

  grpc::ServerUnaryReactor *PubsubLongPolling(
      grpc::CallbackServerContext *context,
      const rpc::PubsubLongPollingRequest *request,
      rpc::PubsubLongPollingReply *reply) override {
    auto *reactor = context->DefaultReactor();
    publisher_->ConnectToSubscriber(*request,
                                    reply->mutable_publisher_id(),
                                    reply->mutable_pub_messages(),
                                    [reactor](ray::Status status,
                                              std::function<void()> success_cb,
                                              std::function<void()> failure_cb) {
                                      // Long polling should always succeed.
                                      RAY_CHECK_OK(status);
                                      reactor->Finish(grpc::Status::OK);
                                    });
    return reactor;
  }

  // For simplicity, all work is done on the GRPC thread.
  grpc::ServerUnaryReactor *PubsubCommandBatch(
      grpc::CallbackServerContext *context,
      const rpc::PubsubCommandBatchRequest *request,
      rpc::PubsubCommandBatchReply *reply) override {
    const auto subscriber_id = UniqueID::FromBinary(request->subscriber_id());
    auto *reactor = context->DefaultReactor();
    for (const auto &command : request->commands()) {
      if (command.has_unsubscribe_message()) {
        publisher_->UnregisterSubscription(command.channel_type(),
                                           subscriber_id,
                                           command.key_id().empty()
                                               ? std::nullopt
                                               : std::make_optional(command.key_id()));
      } else if (command.has_subscribe_message()) {
        publisher_->RegisterSubscription(command.channel_type(),
                                         subscriber_id,
                                         command.key_id().empty()
                                             ? std::nullopt
                                             : std::make_optional(command.key_id()));
      } else {
        RAY_LOG(FATAL)
            << "Invalid command has received, "
            << static_cast<int>(command.command_message_one_of_case())
            << ". If you see this message, please file an issue to Ray Github.";
      }
    }
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  Publisher &GetPublisher() { return *publisher_; }

 private:
  std::unique_ptr<Publisher> publisher_;
};

// Adapts GcsRpcClient to SubscriberClientInterface for making RPC calls. Thread safe.
class CallbackSubscriberClient final : public pubsub::SubscriberClientInterface {
 public:
  explicit CallbackSubscriberClient(const std::string &address) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub_ = rpc::SubscriberService::NewStub(std::move(channel));
  }

  ~CallbackSubscriberClient() final = default;

  void PubsubLongPolling(
      rpc::PubsubLongPollingRequest &&request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) final {
    auto *context = new grpc::ClientContext;
    auto *reply = new rpc::PubsubLongPollingReply;
    stub_->async()->PubsubLongPolling(
        context, &request, reply, [callback, context, reply](grpc::Status s) {
          callback(GrpcStatusToRayStatus(s), std::move(*reply));
          delete reply;
          delete context;
        });
  }

  void PubsubCommandBatch(
      rpc::PubsubCommandBatchRequest &&request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) final {
    auto *context = new grpc::ClientContext;
    auto *reply = new rpc::PubsubCommandBatchReply;
    stub_->async()->PubsubCommandBatch(
        context, &request, reply, [callback, context, reply](grpc::Status s) {
          callback(GrpcStatusToRayStatus(s), std::move(*reply));
          delete reply;
          delete context;
        });
  }

  std::string DebugString() const { return ""; }

 private:
  std::unique_ptr<rpc::SubscriberService::Stub> stub_;
};

class IntegrationTest : public ::testing::Test {
 protected:
  IntegrationTest() {
    // Initialize publisher address.
    address_ = "127.0.0.1:7928";
    address_proto_.set_ip_address("127.0.0.1");
    address_proto_.set_port(7928);
    address_proto_.set_worker_id(UniqueID::FromRandom().Binary());
    io_service_.Run();
    periodical_runner_ = PeriodicalRunner::Create(*io_service_.Get());

    SetupServer();
  }

  ~IntegrationTest() {
    RAY_LOG(INFO) << "Shutting down server.";
    // Stop callback runners.
    io_service_.Stop();
    RAY_LOG(INFO) << "Shutting down server1.";
    // Assume no new subscriber is connected after the unregisteration above. Otherwise
    // shutdown would hang below.
    server_->Shutdown();
  }

  void SetupServer(const std::vector<rpc::ChannelType> &channels = {
                       rpc::ChannelType::GCS_ACTOR_CHANNEL}) {
    if (server_ != nullptr) {
      server_->Shutdown();
    }

    auto publisher = std::make_unique<Publisher>(
        /*channels=*/channels,
        /*periodical_runner=*/*periodical_runner_,
        /*get_time_ms=*/[]() -> double { return absl::ToUnixMicros(absl::Now()); },
        /*subscriber_timeout_ms=*/absl::ToInt64Microseconds(absl::Seconds(30)),
        /*batch_size=*/100);
    subscriber_service_ = std::make_unique<SubscriberServiceImpl>(std::move(publisher));

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
    builder.RegisterService(subscriber_service_.get());
    server_ = builder.BuildAndStart();
  }

  void RestartServer() { SetupServer(); }

  std::unique_ptr<Subscriber> CreateSubscriber(
      const std::vector<rpc::ChannelType> &channels = {
          rpc::ChannelType::GCS_ACTOR_CHANNEL}) {
    return std::make_unique<Subscriber>(
        UniqueID::FromRandom(),
        /*channels=*/channels,
        /*max_command_batch_size=*/3,
        /*get_client=*/
        [](const rpc::Address &address) {
          return std::make_shared<CallbackSubscriberClient>(
              BuildAddress(address.ip_address(), address.port()));
        },
        io_service_.Get());
  }

  std::string address_;
  rpc::Address address_proto_;
  IOServicePool io_service_ = IOServicePool(3);
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  std::unique_ptr<SubscriberServiceImpl> subscriber_service_;
  std::unique_ptr<grpc::Server> server_;
};

TEST_F(IntegrationTest, SubscribersToOneIDAndAllIDs) {
  const std::string subscribed_actor =
      ActorID::FromHex("f4ce02420592ca68c1738a0d01000000").Binary();
  absl::BlockingCounter counter(2);
  absl::Mutex mu;

  std::vector<rpc::ActorTableData> actors_1;
  auto subscriber_1 = CreateSubscriber();
  subscriber_1->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_ACTOR_CHANNEL,
      address_proto_,
      subscribed_actor,
      /*subscribe_done_callback=*/
      [&counter](Status status) {
        RAY_CHECK_OK(status);
        counter.DecrementCount();
      },
      /*subscribe_item_callback=*/
      [&mu, &actors_1](const rpc::PubMessage &msg) {
        absl::MutexLock lock(&mu);
        actors_1.push_back(msg.actor_message());
      },
      /*subscription_failure_callback=*/
      [](const std::string &, const Status &status) { RAY_CHECK_OK(status); });

  std::vector<rpc::ActorTableData> actors_2;
  auto subscriber_2 = CreateSubscriber();
  subscriber_2->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_ACTOR_CHANNEL,
      address_proto_,
      /*key_id=*/std::nullopt,
      /*subscribe_done_callback=*/
      [&counter](Status status) {
        RAY_CHECK_OK(status);
        counter.DecrementCount();
      },
      /*subscribe_item_callback=*/
      [&mu, &actors_2](const rpc::PubMessage &msg) {
        absl::MutexLock lock(&mu);
        actors_2.push_back(msg.actor_message());
      },
      /*subscription_failure_callback=*/
      [](const std::string &, const Status &status) { RAY_CHECK_OK(status); });

  // Wait for subscriptions done before trying to publish.
  counter.Wait();

  rpc::ActorTableData actor_data;
  actor_data.set_actor_id(subscribed_actor);
  actor_data.set_state(rpc::ActorTableData::ALIVE);
  actor_data.set_name("test actor");
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  msg.set_key_id(subscribed_actor);
  *msg.mutable_actor_message() = actor_data;

  subscriber_service_->GetPublisher().Publish(msg);

  absl::MutexLock lock(&mu);

  auto received_id = [&mu, &actors_1]() {
    mu.AssertReaderHeld();  // For annotalysis.
    return actors_1.size() == 1;
  };
  if (!mu.AwaitWithTimeout(absl::Condition(&received_id), absl::Seconds(10))) {
    FAIL() << "Subscriber for actor ID did not receive the published message.";
  }

  auto received_all = [&mu, &actors_2]() {
    mu.AssertReaderHeld();  // For annotalysis.
    return actors_2.size() == 1;
  };
  if (!mu.AwaitWithTimeout(absl::Condition(&received_all), absl::Seconds(10))) {
    FAIL() << "Subscriber for actor channel did not receive the published message.";
  }

  EXPECT_EQ(actors_1[0].actor_id(), actor_data.actor_id());
  EXPECT_EQ(actors_2[0].actor_id(), actor_data.actor_id());

  subscriber_1->Unsubscribe(
      rpc::ChannelType::GCS_ACTOR_CHANNEL, address_proto_, subscribed_actor);
  subscriber_2->Unsubscribe(rpc::ChannelType::GCS_ACTOR_CHANNEL,
                            address_proto_,
                            /*key_id=*/std::nullopt);

  // Waiting here is necessary to avoid invalid memory access during shutdown.
  // TODO(mwtian): cancel inflight polls during subscriber shutdown, and remove the
  // logic below.
  int wait_count = 0;
  while (!(subscriber_1->CheckNoLeaks() && subscriber_2->CheckNoLeaks())) {
    // Flush all the inflight long polling.
    subscriber_service_->GetPublisher().UnregisterSubscriber(
        subscriber_1->subscriber_id_);
    subscriber_service_->GetPublisher().UnregisterSubscriber(
        subscriber_2->subscriber_id_);
    ASSERT_LT(wait_count, 60) << "Subscribers still have inflight operations after 60s";
    ++wait_count;
    absl::SleepFor(absl::Seconds(1));
  }
}

// Test for NodeAddressAndLiveness channel with high node churn.
// This simulates the production scenario where nodes are added, removed, and re-added.
TEST_F(IntegrationTest, NodeAddressAndLivenessHighChurn) {
  // Set up server with the NODE_ADDRESS_AND_LIVENESS channel
  SetupServer({rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL});

  absl::Mutex mu;
  std::vector<rpc::GcsNodeAddressAndLiveness> received_messages;
  absl::BlockingCounter subscribe_counter(1);

  // Create subscriber for the channel
  auto subscriber =
      CreateSubscriber({rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL});
  subscriber->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
      address_proto_,
      /*key_id=*/std::nullopt,
      /*subscribe_done_callback=*/
      [&subscribe_counter](Status status) {
        RAY_CHECK_OK(status);
        subscribe_counter.DecrementCount();
      },
      /*subscribe_item_callback=*/
      [&mu, &received_messages](const rpc::PubMessage &msg) {
        absl::MutexLock lock(&mu);
        received_messages.push_back(msg.node_address_and_liveness_message());
      },
      /*subscription_failure_callback=*/
      [](const std::string &, const Status &status) { RAY_CHECK_OK(status); });

  // Wait for subscription to complete
  subscribe_counter.Wait();

  // Simulate high node churn: 1000 nodes being added, removed, and re-added
  const int num_nodes = 1000;
  std::vector<NodeID> node_ids;
  for (int i = 0; i < num_nodes; i++) {
    node_ids.push_back(NodeID::FromRandom());
  }

  // Phase 1: Add all nodes (ALIVE state)
  RAY_LOG(INFO) << "Publishing " << num_nodes << " ALIVE node messages...";
  for (int i = 0; i < num_nodes; i++) {
    rpc::GcsNodeAddressAndLiveness node_data;
    node_data.set_node_id(node_ids[i].Binary());
    node_data.set_node_manager_address("127.0.0.1");
    node_data.set_node_manager_port(6379 + i);
    node_data.set_object_manager_port(8076 + i);
    node_data.set_state(rpc::GcsNodeInfo::ALIVE);

    rpc::PubMessage msg;
    msg.set_channel_type(rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL);
    msg.set_key_id(node_ids[i].Binary());
    *msg.mutable_node_address_and_liveness_message() = node_data;

    subscriber_service_->GetPublisher().Publish(msg);
  }

  // Phase 2: Mark all nodes as DEAD
  RAY_LOG(INFO) << "Publishing " << num_nodes << " DEAD node messages...";
  for (int i = 0; i < num_nodes; i++) {
    rpc::GcsNodeAddressAndLiveness node_data;
    node_data.set_node_id(node_ids[i].Binary());
    node_data.set_node_manager_address("127.0.0.1");
    node_data.set_node_manager_port(6379 + i);
    node_data.set_object_manager_port(8076 + i);
    node_data.set_state(rpc::GcsNodeInfo::DEAD);

    rpc::PubMessage msg;
    msg.set_channel_type(rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL);
    msg.set_key_id(node_ids[i].Binary());
    *msg.mutable_node_address_and_liveness_message() = node_data;

    subscriber_service_->GetPublisher().Publish(msg);
  }

  // Phase 3: Re-add all nodes as ALIVE
  RAY_LOG(INFO) << "Publishing " << num_nodes << " re-added ALIVE node messages...";
  for (int i = 0; i < num_nodes; i++) {
    rpc::GcsNodeAddressAndLiveness node_data;
    node_data.set_node_id(node_ids[i].Binary());
    node_data.set_node_manager_address("127.0.0.1");
    node_data.set_node_manager_port(6379 + i);
    node_data.set_object_manager_port(8076 + i);
    node_data.set_state(rpc::GcsNodeInfo::ALIVE);

    rpc::PubMessage msg;
    msg.set_channel_type(rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL);
    msg.set_key_id(node_ids[i].Binary());
    *msg.mutable_node_address_and_liveness_message() = node_data;

    subscriber_service_->GetPublisher().Publish(msg);
  }

  // Wait for all messages to be received
  const size_t expected_messages = num_nodes * 3;  // ALIVE + DEAD + ALIVE
  RAY_LOG(INFO) << "Waiting for " << expected_messages << " messages...";

  absl::MutexLock lock(&mu);
  auto all_received = [&mu, &received_messages]() {
    mu.AssertReaderHeld();
    return received_messages.size() >= static_cast<size_t>(num_nodes * 3);
  };
  if (!mu.AwaitWithTimeout(absl::Condition(&all_received), absl::Seconds(60))) {
    FAIL() << "Expected " << expected_messages << " messages, but received only "
           << received_messages.size();
  }

  RAY_LOG(INFO) << "Received all " << received_messages.size() << " messages.";

  // Verify that we received all expected messages
  EXPECT_EQ(received_messages.size(), static_cast<size_t>(expected_messages));

  // Verify the sequence: for each node, we should see ALIVE -> DEAD -> ALIVE
  std::map<std::string, std::vector<rpc::GcsNodeInfo::GcsNodeState>> node_states;
  for (const auto &msg : received_messages) {
    node_states[msg.node_id()].push_back(msg.state());
  }

  EXPECT_EQ(node_states.size(), static_cast<size_t>(num_nodes));
  for (const auto &[node_id, states] : node_states) {
    EXPECT_EQ(states.size(), 3u) << "Node " << NodeID::FromBinary(node_id).Hex()
                                 << " should have 3 state transitions";
    if (states.size() == 3) {
      EXPECT_EQ(states[0], rpc::GcsNodeInfo::ALIVE)
          << "Node " << NodeID::FromBinary(node_id).Hex()
          << " first state should be ALIVE";
      EXPECT_EQ(states[1], rpc::GcsNodeInfo::DEAD)
          << "Node " << NodeID::FromBinary(node_id).Hex()
          << " second state should be DEAD";
      EXPECT_EQ(states[2], rpc::GcsNodeInfo::ALIVE)
          << "Node " << NodeID::FromBinary(node_id).Hex()
          << " third state should be ALIVE";
    }
  }

  // Clean up
  subscriber->Unsubscribe(rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
                          address_proto_,
                          /*key_id=*/std::nullopt);

  int wait_count = 0;
  while (!subscriber->CheckNoLeaks()) {
    subscriber_service_->GetPublisher().UnregisterSubscriber(subscriber->subscriber_id_);
    ASSERT_LT(wait_count, 60) << "Subscriber still has inflight operations after 60s";
    ++wait_count;
    absl::SleepFor(absl::Seconds(1));
  }
}

}  // namespace pubsub
}  // namespace ray
