// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/gcs_node_manager.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/network_util.h"
#include "src/ray/protobuf/pubsub.grpc.pb.h"

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {
 public:
  GcsNodeManagerTest() {
    auto raylet_client = std::make_shared<rpc::FakeRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [raylet_client = std::move(raylet_client)](const rpc::Address &) {
          return raylet_client;
        });
    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_unique<gcs::GcsTableStorage>(
        std::make_shared<gcs::InMemoryStoreClient>());
    io_context_ = std::make_unique<instrumented_io_context>("GcsNodeManagerTest");
    fake_ray_event_recorder_ = std::make_unique<observability::FakeRayEventRecorder>();
  }

 protected:
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<rpc::RayletClientPool> client_pool_;
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<instrumented_io_context> io_context_;
  std::unique_ptr<observability::FakeRayEventRecorder> fake_ray_event_recorder_;
};

TEST_F(GcsNodeManagerTest, TestRayEventNodeEvents) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true
}
)");
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");
  auto node = GenNodeInfo();
  rpc::RegisterNodeRequest register_request;
  register_request.mutable_node_info()->CopyFrom(*node);
  rpc::RegisterNodeReply register_reply;
  auto send_register_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  // Add a node to the manager
  node_manager.HandleRegisterNode(
      register_request, &register_reply, send_register_reply_callback);
  // Exhaust the event loop
  while (io_context_->poll() > 0) {
  }
  auto register_events = fake_ray_event_recorder_->FlushBuffer();

  // Test the node definition event + alive node lifecycle event
  ASSERT_EQ(register_events.size(), 2);
  auto ray_event_0 = std::move(*register_events[0]).Serialize();
  auto ray_event_1 = std::move(*register_events[1]).Serialize();
  ASSERT_EQ(ray_event_0.event_type(), rpc::events::RayEvent::NODE_DEFINITION_EVENT);
  ASSERT_EQ(ray_event_0.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(ray_event_0.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(ray_event_0.session_name(), "test_session_name");
  ASSERT_EQ(ray_event_0.node_definition_event().node_id(), node->node_id());
  ASSERT_EQ(ray_event_0.node_definition_event().node_ip_address(),
            node->node_manager_address());
  ASSERT_EQ(ray_event_0.node_definition_event().start_timestamp().seconds(),
            node->start_time_ms() / 1000);
  std::map<std::string, std::string> event_labels(
      ray_event_0.node_definition_event().labels().begin(),
      ray_event_0.node_definition_event().labels().end());
  std::map<std::string, std::string> node_labels(node->labels().begin(),
                                                 node->labels().end());
  ASSERT_EQ(event_labels, node_labels);
  ASSERT_EQ(ray_event_1.event_type(), rpc::events::RayEvent::NODE_LIFECYCLE_EVENT);
  ASSERT_EQ(ray_event_1.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(ray_event_1.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(ray_event_1.session_name(), "test_session_name");
  ASSERT_EQ(ray_event_1.node_lifecycle_event().node_id(), node->node_id());
  ASSERT_EQ(ray_event_1.node_lifecycle_event().state_transitions(0).state(),
            rpc::events::NodeLifecycleEvent::ALIVE);
  ASSERT_EQ(ray_event_1.node_lifecycle_event().state_transitions(0).alive_sub_state(),
            rpc::events::NodeLifecycleEvent::UNSPECIFIED);

  // Test drain node lifecycle event only export one event
  rpc::syncer::ResourceViewSyncMessage sync_message;
  sync_message.set_is_draining(true);
  node_manager.UpdateAliveNode(NodeID::FromBinary(node->node_id()), sync_message);
  node_manager.UpdateAliveNode(NodeID::FromBinary(node->node_id()), sync_message);
  auto drain_events = fake_ray_event_recorder_->FlushBuffer();
  ASSERT_EQ(drain_events.size(), 1);
  auto ray_event_02 = std::move(*drain_events[0]).Serialize();
  ASSERT_EQ(ray_event_02.event_type(), rpc::events::RayEvent::NODE_LIFECYCLE_EVENT);
  ASSERT_EQ(ray_event_02.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(ray_event_02.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(ray_event_02.session_name(), "test_session_name");
  ASSERT_EQ(ray_event_02.node_lifecycle_event().node_id(), node->node_id());
  ASSERT_EQ(ray_event_02.node_lifecycle_event().state_transitions(0).state(),
            rpc::events::NodeLifecycleEvent::ALIVE);
  ASSERT_EQ(ray_event_02.node_lifecycle_event().state_transitions(0).alive_sub_state(),
            rpc::events::NodeLifecycleEvent::DRAINING);

  // Remove the node from the manager
  rpc::UnregisterNodeRequest unregister_request;
  unregister_request.set_node_id(node->node_id());
  unregister_request.mutable_node_death_info()->set_reason(
      rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  unregister_request.mutable_node_death_info()->set_reason_message("mock reason message");
  rpc::UnregisterNodeReply unregister_reply;
  auto send_unregister_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  node_manager.HandleUnregisterNode(
      unregister_request, &unregister_reply, send_unregister_reply_callback);
  // Exhaust the event loop
  while (io_context_->poll() > 0) {
  }

  // Test the dead node lifecycle event
  auto unregister_events = fake_ray_event_recorder_->FlushBuffer();
  ASSERT_EQ(unregister_events.size(), 1);
  auto ray_event_03 = std::move(*unregister_events[0]).Serialize();
  ASSERT_EQ(ray_event_03.event_type(), rpc::events::RayEvent::NODE_LIFECYCLE_EVENT);
  ASSERT_EQ(ray_event_03.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(ray_event_03.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(ray_event_03.session_name(), "test_session_name");
  ASSERT_EQ(ray_event_03.node_lifecycle_event().node_id(), node->node_id());
  ASSERT_EQ(ray_event_03.node_lifecycle_event().state_transitions(0).state(),
            rpc::events::NodeLifecycleEvent::DEAD);
  ASSERT_EQ(
      ray_event_03.node_lifecycle_event().state_transitions(0).death_info().reason(),
      rpc::events::NodeLifecycleEvent::DeathInfo::EXPECTED_TERMINATION);
  ASSERT_EQ(ray_event_03.node_lifecycle_event()
                .state_transitions(0)
                .death_info()
                .reason_message(),
            "mock reason message");
}

TEST_F(GcsNodeManagerTest, TestManagement) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");
  // Test Add/Get/Remove functionality.
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetAliveNode(node_id).value());

  rpc::NodeDeathInfo death_info;
  node_manager.RemoveNode(node_id, death_info, rpc::GcsNodeInfo::DEAD, 1000);
  ASSERT_TRUE(!node_manager.GetAliveNode(node_id).has_value());
}

TEST_F(GcsNodeManagerTest, TestListener) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");
  // Test AddNodeAddedListener.
  int node_count = 1000;
  std::atomic_int callbacks_remaining = node_count;

  std::vector<std::shared_ptr<const rpc::GcsNodeInfo>> added_nodes;
  node_manager.AddNodeAddedListener(
      [&added_nodes, &callbacks_remaining](std::shared_ptr<const rpc::GcsNodeInfo> node) {
        added_nodes.emplace_back(std::move(node));
        --callbacks_remaining;
      },
      *io_context_);
  for (int i = 0; i < node_count; ++i) {
    auto node = GenNodeInfo();
    node_manager.AddNode(node);
  }

  // Block until all callbacks have processed
  while (callbacks_remaining > 0) {
    io_context_->run_one();
  }

  ASSERT_EQ(node_count, added_nodes.size());

  // Test GetAllAliveNodes.
  auto alive_nodes = node_manager.GetAllAliveNodes();
  ASSERT_EQ(added_nodes.size(), alive_nodes.size());
  for (const auto &node : added_nodes) {
    ASSERT_EQ(1, alive_nodes.count(NodeID::FromBinary(node->node_id())));
  }

  // Test AddNodeRemovedListener.

  // reset the counter
  callbacks_remaining = node_count;
  std::vector<std::shared_ptr<const rpc::GcsNodeInfo>> removed_nodes;
  node_manager.AddNodeRemovedListener(
      [&removed_nodes,
       &callbacks_remaining](std::shared_ptr<const rpc::GcsNodeInfo> node) {
        removed_nodes.emplace_back(std::move(node));
        --callbacks_remaining;
      },
      *io_context_);
  rpc::NodeDeathInfo death_info;
  for (int i = 0; i < node_count; ++i) {
    node_manager.RemoveNode(NodeID::FromBinary(added_nodes[i]->node_id()),
                            death_info,
                            rpc::GcsNodeInfo::DEAD,
                            1000);
  }

  // Block until all callbacks have processed
  while (callbacks_remaining > 0) {
    io_context_->run_one();
  }

  ASSERT_EQ(node_count, removed_nodes.size());
  ASSERT_TRUE(node_manager.GetAllAliveNodes().empty());
  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(added_nodes[i]->node_id(), removed_nodes[i]->node_id());
  }
}

// Register a node-added listener that calls back into
// GcsNodeManager::IsNodeAlive(node_id) during notification. Verify no deadlock and that
// state remains consistent. This validates the "post-notify" approach.

TEST_F(GcsNodeManagerTest, TestAddNodeListenerCallbackDeadlock) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");
  int node_count = 10;
  std::atomic_int callbacks_remaining = node_count;
  node_manager.AddNodeAddedListener(
      [&node_manager,
       &callbacks_remaining](std::shared_ptr<const rpc::GcsNodeInfo> node) {
        rpc::NodeDeathInfo death_info;
        node_manager.RemoveNode(NodeID::FromBinary(node->node_id()),
                                death_info,
                                rpc::GcsNodeInfo::DEAD,
                                1000);
        --callbacks_remaining;
      },
      *io_context_);
  for (int i = 0; i < node_count; ++i) {
    auto node = GenNodeInfo();
    node_manager.AddNode(node);
  }
  while (callbacks_remaining > 0) {
    io_context_->run_one();
  }
  ASSERT_EQ(0, node_manager.GetAllAliveNodes().size());
}

TEST_F(GcsNodeManagerTest, TestUpdateAliveNode) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");

  // Create a test node
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  // Add the node to the manager
  node_manager.AddNode(node);

  // Test 1: Update node with idle state
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(5000);

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(), rpc::NodeSnapshot::IDLE);
    EXPECT_EQ(updated_node.value()->state_snapshot().idle_duration_ms(), 5000);
  }

  // Test 2: Update node with active state (idle_duration_ms = 0)
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(0);
    sync_message.add_node_activity("Busy workers on node.");

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(), rpc::NodeSnapshot::ACTIVE);
    EXPECT_EQ(updated_node.value()->state_snapshot().node_activity_size(), 1);
    EXPECT_EQ(updated_node.value()->state_snapshot().node_activity(0),
              "Busy workers on node.");
  }

  // Test 3: Update node with draining state
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(0);
    sync_message.set_is_draining(true);

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(),
              rpc::NodeSnapshot::DRAINING);
  }

  // Test 4: Update node with draining state with activity and idle duration (new activity
  // should be ignored)
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(100);
    sync_message.set_is_draining(true);
    sync_message.add_node_activity("Very Busy workers on node.");
    sync_message.add_node_activity("Oh such very very busy workers on node.");

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(),
              rpc::NodeSnapshot::DRAINING);
    EXPECT_FALSE(updated_node.value()->state_snapshot().node_activity_size() == 1);
    EXPECT_EQ(updated_node.value()->state_snapshot().idle_duration_ms(), 100);
  }
}

TEST_F(GcsNodeManagerTest, TestGetNodeAddressAndLiveness) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");

  // Create and add a test node
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  node_manager.AddNode(node);

  // Test getting address and liveness for existing alive node
  auto address_and_liveness = node_manager.GetAliveNodeAddress(node_id);
  ASSERT_TRUE(address_and_liveness.has_value());
  EXPECT_EQ(address_and_liveness.value().node_id(), node->node_id());
  EXPECT_EQ(address_and_liveness.value().node_manager_address(),
            node->node_manager_address());
  EXPECT_EQ(address_and_liveness.value().node_manager_port(), node->node_manager_port());
  EXPECT_EQ(address_and_liveness.value().object_manager_port(),
            node->object_manager_port());
  EXPECT_EQ(address_and_liveness.value().state(), rpc::GcsNodeInfo::ALIVE);

  // Test getting address and liveness for non-existent node
  auto non_existent_node_id = NodeID::FromRandom();
  auto non_existent_result = node_manager.GetAliveNodeAddress(non_existent_node_id);
  EXPECT_FALSE(non_existent_result.has_value());

  // Remove the node and verify it's no longer accessible
  rpc::NodeDeathInfo death_info;
  death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  node_manager.RemoveNode(node_id, death_info, rpc::GcsNodeInfo::DEAD, 1000);

  auto removed_result = node_manager.GetAliveNodeAddress(node_id);
  EXPECT_FALSE(removed_result.has_value());
}

TEST_F(GcsNodeManagerTest, TestHandleGetAllNodeAddressAndLiveness) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");

  // Add multiple alive nodes
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes;
  for (int i = 0; i < 5; ++i) {
    auto node = GenNodeInfo();
    node->set_node_name("node_" + std::to_string(i));
    alive_nodes.push_back(node);
    node_manager.AddNode(node);
  }

  // Add some dead nodes
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> dead_nodes;
  for (int i = 0; i < 3; ++i) {
    auto node = GenNodeInfo();
    node->set_node_name("dead_node_" + std::to_string(i));
    dead_nodes.push_back(node);
    node_manager.AddNode(node);
    rpc::UnregisterNodeRequest unregister_request;
    unregister_request.set_node_id(node->node_id());
    unregister_request.mutable_node_death_info()->set_reason(
        rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    rpc::UnregisterNodeReply unregister_reply;
    unregister_request.mutable_node_death_info()->set_reason_message(
        "mock reason message");
    auto send_unregister_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {
          // NoOp
        };
    node_manager.HandleUnregisterNode(
        unregister_request, &unregister_reply, send_unregister_reply_callback);
    while (io_context_->poll() > 0)
      ;
  }

  // Test 1: Get all nodes without filter
  {
    absl::flat_hash_set<NodeID> node_ids;  // empty = all nodes
    std::vector<rpc::GcsNodeAddressAndLiveness> result;
    node_manager.GetAllNodeAddressAndLiveness(
        node_ids,
        std::nullopt,
        std::numeric_limits<int64_t>::max(),
        [&result](rpc::GcsNodeAddressAndLiveness &&node) {
          result.push_back(std::move(node));
        });

    EXPECT_EQ(result.size(), 8);  // 5 alive + 3 dead
  }

  // Test 2: Get only alive nodes
  {
    absl::flat_hash_set<NodeID> node_ids;  // empty = all nodes
    std::vector<rpc::GcsNodeAddressAndLiveness> result;
    node_manager.GetAllNodeAddressAndLiveness(
        node_ids,
        rpc::GcsNodeInfo::ALIVE,
        std::numeric_limits<int64_t>::max(),
        [&result](rpc::GcsNodeAddressAndLiveness &&node) {
          result.push_back(std::move(node));
        });

    EXPECT_EQ(result.size(), 5);

    // Verify all returned nodes are alive
    for (const auto &node_info : result) {
      EXPECT_EQ(node_info.state(), rpc::GcsNodeInfo::ALIVE);
    }
  }

  // Test 3: Get only dead nodes
  {
    absl::flat_hash_set<NodeID> node_ids;  // empty = all nodes
    std::vector<rpc::GcsNodeAddressAndLiveness> result;
    node_manager.GetAllNodeAddressAndLiveness(
        node_ids,
        rpc::GcsNodeInfo::DEAD,
        std::numeric_limits<int64_t>::max(),
        [&result](rpc::GcsNodeAddressAndLiveness &&node) {
          result.push_back(std::move(node));
        });

    EXPECT_EQ(result.size(), 3);

    // Verify all returned nodes are dead
    for (const auto &node_info : result) {
      EXPECT_EQ(node_info.state(), rpc::GcsNodeInfo::DEAD);
      EXPECT_EQ(node_info.death_info().reason(),
                rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    }
  }

  // Test 4: Filter by specific node ID
  {
    absl::flat_hash_set<NodeID> node_ids;
    node_ids.insert(NodeID::FromBinary(alive_nodes[0]->node_id()));
    std::vector<rpc::GcsNodeAddressAndLiveness> result;
    node_manager.GetAllNodeAddressAndLiveness(
        node_ids,
        std::nullopt,
        std::numeric_limits<int64_t>::max(),
        [&result](rpc::GcsNodeAddressAndLiveness &&node) {
          result.push_back(std::move(node));
        });

    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].node_id(), alive_nodes[0]->node_id());
  }

  // Test 5: Apply limit
  {
    absl::flat_hash_set<NodeID> node_ids;  // empty = all nodes
    std::vector<rpc::GcsNodeAddressAndLiveness> result;
    node_manager.GetAllNodeAddressAndLiveness(
        node_ids, std::nullopt, 3, [&result](rpc::GcsNodeAddressAndLiveness &&node) {
          result.push_back(std::move(node));
        });

    EXPECT_EQ(result.size(), 3);
  }
}

// Subscriber service implementation for integration testing
class SubscriberServiceImpl final : public rpc::SubscriberService::CallbackService {
 public:
  explicit SubscriberServiceImpl(std::unique_ptr<pubsub::Publisher> publisher)
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
                                      RAY_CHECK_OK(status);
                                      reactor->Finish(grpc::Status::OK);
                                    });
    return reactor;
  }

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
      }
    }
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  pubsub::Publisher &GetPublisher() { return *publisher_; }

 private:
  std::unique_ptr<pubsub::Publisher> publisher_;
};

// Subscriber client for integration testing
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

// Integration test for GCS NodeManager with NodeAddressAndLiveness pubsub
TEST_F(GcsNodeManagerTest, TestNodeAddressAndLivenessHighChurn) {
  // Set up pubsub infrastructure
  IOServicePool io_service_pool(3);
  io_service_pool.Run();
  auto periodical_runner = PeriodicalRunner::Create(*io_service_pool.Get());

  const std::string address = "127.0.0.1:7929";
  rpc::Address address_proto;
  address_proto.set_ip_address("127.0.0.1");
  address_proto.set_port(7929);
  address_proto.set_worker_id(UniqueID::FromRandom().Binary());

  // Create publisher - we'll keep this alive throughout the test
  std::shared_ptr<pubsub::Publisher> publisher = std::make_shared<pubsub::Publisher>(
      /*channels=*/
      std::vector<rpc::ChannelType>{
          rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
          rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
      },
      /*periodical_runner=*/*periodical_runner,
      /*get_time_ms=*/[]() -> double { return absl::ToUnixMicros(absl::Now()); },
      /*subscriber_timeout_ms=*/absl::ToInt64Microseconds(absl::Seconds(30)),
      /*batch_size=*/100);

  // Non-owning deleter struct
  struct NoOpDeleter {
    void operator()(pubsub::Publisher *) const {}
    void operator()(pubsub::PublisherInterface *) const {}
  };

  // Set up gRPC server with a non-owning reference to the publisher
  auto subscriber_service = std::make_unique<SubscriberServiceImpl>(
      std::unique_ptr<pubsub::Publisher, NoOpDeleter>(publisher.get()));
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(subscriber_service.get());
  auto server = builder.BuildAndStart();

  // Create GCS publisher that wraps the real pubsub publisher (also non-owning)
  auto gcs_publisher_wrapper = std::make_unique<pubsub::GcsPublisher>(
      std::unique_ptr<pubsub::PublisherInterface, NoOpDeleter>(publisher.get()));

  // Create GCS NodeManager with the real publisher
  gcs::GcsNodeManager node_manager(gcs_publisher_wrapper.get(),
                                   gcs_table_storage_.get(),
                                   *io_context_,
                                   client_pool_.get(),
                                   ClusterID::Nil(),
                                   *fake_ray_event_recorder_,
                                   "test_session_name");

  // Create subscriber
  auto subscriber = std::make_unique<pubsub::Subscriber>(
      UniqueID::FromRandom(),
      /*channels=*/
      std::vector<rpc::ChannelType>{
          rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
      },
      /*max_command_batch_size=*/3,
      /*get_client=*/
      [](const rpc::Address &address) {
        return std::make_shared<CallbackSubscriberClient>(
            BuildAddress(address.ip_address(), address.port()));
      },
      io_service_pool.Get());

  // Track received messages
  absl::Mutex mu;
  std::vector<rpc::GcsNodeAddressAndLiveness> received_messages;

  // Subscribe to NodeAddressAndLiveness channel
  std::atomic<bool> subscribed(false);
  subscriber->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
      address_proto,
      /*key_id=*/std::nullopt,
      /*subscribe_done_callback=*/
      [&subscribed](Status status) {
        RAY_CHECK_OK(status);
        subscribed = true;
      },
      /*subscribe_item_callback=*/
      [&mu, &received_messages](const rpc::PubMessage &msg) {
        absl::MutexLock lock(&mu);
        received_messages.push_back(msg.node_address_and_liveness_message());
      },
      /*subscription_failure_callback=*/
      [](const std::string &, const Status &status) { RAY_CHECK_OK(status); });

  // Wait for subscription to complete
  while (!subscribed) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Simulate high node churn: 1000 nodes being added, removed, and re-added
  const int num_nodes = 1000;
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodes;
  for (int i = 0; i < num_nodes; i++) {
    auto node = GenNodeInfo();
    nodes.push_back(node);
  }

  RAY_LOG(INFO) << "Adding " << num_nodes << " nodes...";
  // Phase 1: Add all nodes
  for (int i = 0; i < num_nodes; i++) {
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(*nodes[i]);
    rpc::RegisterNodeReply register_reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    node_manager.HandleRegisterNode(
        register_request, &register_reply, send_reply_callback);
    // Process async operations
    while (io_context_->poll() > 0) {
    }
  }

  RAY_LOG(INFO) << "Removing " << num_nodes << " nodes...";
  // Phase 2: Remove all nodes
  for (int i = 0; i < num_nodes; i++) {
    rpc::UnregisterNodeRequest unregister_request;
    unregister_request.set_node_id(nodes[i]->node_id());
    unregister_request.mutable_node_death_info()->set_reason(
        rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    rpc::UnregisterNodeReply unregister_reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    node_manager.HandleUnregisterNode(
        unregister_request, &unregister_reply, send_reply_callback);
    // Process async operations
    while (io_context_->poll() > 0) {
    }
  }

  RAY_LOG(INFO) << "Re-adding " << num_nodes << " nodes...";
  // Phase 3: Re-add all nodes
  for (int i = 0; i < num_nodes; i++) {
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(*nodes[i]);
    rpc::RegisterNodeReply register_reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    node_manager.HandleRegisterNode(
        register_request, &register_reply, send_reply_callback);
    // Process async operations
    while (io_context_->poll() > 0) {
    }
  }

  // Wait for all messages to be received
  const size_t expected_messages = num_nodes * 3;  // ALIVE + DEAD + ALIVE
  RAY_LOG(INFO) << "Waiting for " << expected_messages << " messages...";

  // Wait up to 60 seconds for all messages
  auto start_time = std::chrono::steady_clock::now();
  while (true) {
    {
      absl::MutexLock lock(&mu);
      if (received_messages.size() >= expected_messages) {
        break;
      }
    }
    auto elapsed = std::chrono::steady_clock::now() - start_time;
    if (elapsed > std::chrono::seconds(60)) {
      absl::MutexLock lock(&mu);
      FAIL() << "Timeout: Expected " << expected_messages
             << " messages, but received only " << received_messages.size();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  absl::MutexLock lock(&mu);
  RAY_LOG(INFO) << "Received all " << received_messages.size() << " messages.";

  // Verify that we received all expected messages
  EXPECT_EQ(received_messages.size(), expected_messages);

  // Verify the sequence: for each node, we should see ALIVE -> DEAD -> ALIVE
  std::map<std::string, std::vector<rpc::GcsNodeInfo::GcsNodeState>> node_states;
  for (const auto &msg : received_messages) {
    node_states[msg.node_id()].push_back(msg.state());
  }

  EXPECT_EQ(node_states.size(), static_cast<size_t>(num_nodes));
  int correct_sequences = 0;
  for (const auto &[node_id, states] : node_states) {
    if (states.size() == 3 && states[0] == rpc::GcsNodeInfo::ALIVE &&
        states[1] == rpc::GcsNodeInfo::DEAD && states[2] == rpc::GcsNodeInfo::ALIVE) {
      correct_sequences++;
    } else {
      RAY_LOG(ERROR)
          << "Node " << NodeID::FromBinary(node_id).Hex()
          << " has incorrect state sequence. Size: " << states.size() << " States: "
          << (states.size() > 0 ? rpc::GcsNodeInfo::GcsNodeState_Name(states[0]) : "none")
          << " -> "
          << (states.size() > 1 ? rpc::GcsNodeInfo::GcsNodeState_Name(states[1]) : "none")
          << " -> "
          << (states.size() > 2 ? rpc::GcsNodeInfo::GcsNodeState_Name(states[2])
                                : "none");
    }
  }

  EXPECT_EQ(correct_sequences, num_nodes)
      << "Only " << correct_sequences << " out of " << num_nodes
      << " nodes have correct ALIVE->DEAD->ALIVE sequence";

  // Clean up
  server->Shutdown();
  io_service_pool.Stop();
}

}  // namespace ray
