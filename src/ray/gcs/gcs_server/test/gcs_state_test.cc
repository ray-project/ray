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

// clang-format off
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/store_client_kv.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "mock/ray/gcs/gcs_server/gcs_table_storage.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "mock/ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/pubsub/gcs_pub_sub.h"

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"
// clang-format on

namespace ray {

namespace gcs {
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using ResourceBundleMap = std::unordered_map<std::string, double>;
using BundlesOnNodeMap = absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>;

// Test suite for AutoscalerState related functionality.
class GcsStateTest : public ::testing::Test {
 public:
  GcsStateTest() {}

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<MockRayletClientInterface> raylet_client_;
  std::shared_ptr<rpc::NodeManagerClientPool> client_pool_;
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
  std::unique_ptr<GcsAutoscalerStateManager> gcs_autoscaler_state_manager_;
  std::shared_ptr<MockGcsPlacementGroupManager> gcs_placement_group_manager_;
  std::shared_ptr<MockGcsTableStorage> gcs_table_storage_;
  std::unique_ptr<RuntimeEnvManager> runtime_env_manager_;
  std::unique_ptr<GcsFunctionManager> function_manager_;
  std::shared_ptr<GcsActorManager> gcs_actor_manager_;
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  std::shared_ptr<MockGcsPublisher> gcs_publisher_;
  std::shared_ptr<GcsActorSchedulerInterface> gcs_actor_scheduler_;

  void SetUp() override {
    raylet_client_ = std::make_shared<MockRayletClientInterface>();
    client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &) { return raylet_client_; });
    cluster_resource_manager_ = std::make_unique<ClusterResourceManager>(io_service_);
    gcs_table_storage_ = std::make_shared<MockGcsTableStorage>();
    gcs_publisher_ = std::make_shared<MockGcsPublisher>();
    gcs_node_manager_ = std::make_shared<GcsNodeManager>(
        gcs_publisher_, gcs_table_storage_, client_pool_, ClusterID::Nil());
    gcs_resource_manager_ =
        std::make_shared<GcsResourceManager>(io_service_,
                                             *cluster_resource_manager_,
                                             *gcs_node_manager_,
                                             NodeID::FromRandom());

    gcs_placement_group_manager_ =
        std::make_shared<MockGcsPlacementGroupManager>(*gcs_resource_manager_);
    gcs_autoscaler_state_manager_.reset(new GcsAutoscalerStateManager(
        "fake_cluster", *gcs_node_manager_, *gcs_placement_group_manager_, client_pool_));

    runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
        [](const std::string &, std::function<void(bool)>) {});
    kv_manager_ = std::make_unique<GcsInternalKVManager>(
        std::make_unique<StoreClientInternalKV>(std::make_unique<MockStoreClient>()));
    function_manager_ = std::make_unique<GcsFunctionManager>(kv_manager_->GetInstance());
    MockGcsActorTable gcs_actor_table;
    gcs_actor_scheduler_ = std::make_shared<MockGcsActorScheduler>(
      io_service_,
      gcs_actor_table,
      *gcs_node_manager_);
    gcs_actor_manager_ = std::make_shared<GcsActorManager>(
        gcs_actor_scheduler_,
        gcs_table_storage_,
        gcs_publisher_,
        *gcs_node_manager_,
        *runtime_env_manager_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        [](const rpc::Address &address) { return nullptr; });
  }

 public:
  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_autoscaler_state_manager_->OnNodeAdd(*node);
  }
};

TEST_F(GcsStateTest, TestDrainNode) {
  // TODO create an actor on this node
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);
  const NodeID node_id = NodeID::FromBinary(node->node_id());
  auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
  ASSERT_TRUE(maybe_node_info.has_value());
  ASSERT_EQ(maybe_node_info.value()->state(), rpc::GcsNodeInfo::ALIVE);

  ON_CALL(*raylet_client_, DrainRaylet)
      .WillByDefault(Invoke(
          [this, node_id](const rpc::autoscaler::DrainNodeReason &reason,
                          const std::string &reason_message,
                          const rpc::ClientCallback<rpc::DrainRayletReply> &callback) {
            auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
            // Check node alive.
            ASSERT_TRUE(maybe_node_info.has_value());
            ASSERT_EQ(maybe_node_info.value()->state(), rpc::GcsNodeInfo::ALIVE);
            rpc::DrainRayletReply reply;
            reply.set_is_accepted(true);
            callback(Status::OK(), reply);
          }));

  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(node->node_id());
  request.set_reason(rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  request.set_reason_message("preemption");
  rpc::autoscaler::DrainNodeReply reply;
  auto send_reply_callback = [this, node_id](ray::Status status,
                                             std::function<void()> f1,
                                             std::function<void()> f2) {
    // Check that we have the proper state after calling drain.
    auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
    ASSERT_TRUE(maybe_node_info.has_value());
    auto node_info = maybe_node_info.value();
    ASSERT_EQ(node_info->state(), rpc::GcsNodeInfo::ALIVE);
    ASSERT_EQ(node_info->death_info().reason(), rpc::NodeDeathInfo::AUTOSCALER_DRAIN);
    ASSERT_EQ(node_info->death_info().drain_reason(),
              rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  };
  gcs_autoscaler_state_manager_->HandleDrainNode(request, &reply, send_reply_callback);
  ASSERT_TRUE(reply.is_accepted());

  // This is normally in GCS.
  gcs_node_manager_->AddNodeRemovedListener(
      [this](std::shared_ptr<rpc::GcsNodeInfo> node) {
        auto node_id = NodeID::FromBinary(node->node_id());
        const auto node_ip_address = node->node_manager_address();
        gcs_actor_manager_->OnNodeDead(node_id, node_ip_address);
      });

  // Simulate raylet failure
  gcs_node_manager_->OnNodeFailure(node_id);
  ON_CALL(*gcs_publisher_, PublishError)
      .WillByDefault(Return(Status::OK()));

  // Expect gcs_table_storage_.Put(actor_id, mutable_actor_table_data, *)
  // mutable_actor_table_data.death_cause = (preempted)
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
