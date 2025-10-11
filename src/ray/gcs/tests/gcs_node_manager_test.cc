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

#include <memory>
#include <utility>
#include <vector>

#include "mock/ray/pubsub/publisher.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

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
                                   *io_context_.get(),
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

}  // namespace ray
