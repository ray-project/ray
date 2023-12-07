// Copyright 2023 The Ray Authors.
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
using ::testing::ReturnRef;

using ResourceBundleMap = std::unordered_map<std::string, double>;
using BundlesOnNodeMap = absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>;

class MockGcsActorManager : public GcsActorManager {
 public:
  MockGcsActorManager(
      std::shared_ptr<GcsActorSchedulerInterface> scheduler,
      std::shared_ptr<GcsTableStorage> gcs_table_storage,
      std::shared_ptr<GcsPublisher> gcs_publisher,
      RuntimeEnvManager &runtime_env_manager,
      GcsFunctionManager &function_manager,
      std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed,
      const rpc::ClientFactoryFn &worker_client_factory = nullptr)
      : GcsActorManager(scheduler,
                        gcs_table_storage,
                        gcs_publisher,
                        runtime_env_manager,
                        function_manager,
                        destroy_owned_placement_group_if_needed,
                        worker_client_factory) {}

  MOCK_METHOD(void,
              PollOwnerForActorOutOfScope,
              (const std::shared_ptr<GcsActor> &actor),
              (override));
};

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
  std::shared_ptr<MockGcsActorManager> gcs_actor_manager_;
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

    runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
        [](const std::string &, std::function<void(bool)>) {});
    kv_manager_ = std::make_unique<GcsInternalKVManager>(
        std::make_unique<StoreClientInternalKV>(std::make_unique<MockStoreClient>()));
    function_manager_ = std::make_unique<GcsFunctionManager>(kv_manager_->GetInstance());
    MockGcsActorTable gcs_actor_table;
    gcs_actor_scheduler_ = std::make_shared<MockGcsActorScheduler>(
        io_service_, gcs_actor_table, *gcs_node_manager_);
    gcs_actor_manager_ = std::make_shared<MockGcsActorManager>(
        gcs_actor_scheduler_,
        gcs_table_storage_,
        gcs_publisher_,
        *runtime_env_manager_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        [](const rpc::Address &address) { return nullptr; });

    gcs_placement_group_manager_ =
        std::make_shared<MockGcsPlacementGroupManager>(*gcs_resource_manager_);
    gcs_autoscaler_state_manager_.reset(
        new GcsAutoscalerStateManager("fake_cluster",
                                      *gcs_node_manager_,
                                      *gcs_actor_manager_,
                                      *gcs_placement_group_manager_,
                                      client_pool_));
  }

 public:
  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_autoscaler_state_manager_->OnNodeAdd(*node);
  }

  void AddActor(const std::string &actor_name) {
    // Create the actor spec and RegisterActor request.
    rpc::RegisterActorRequest request;
    rpc::RegisterActorReply reply;
    auto spec = request.mutable_task_spec();
    spec->set_name(actor_name);
    spec->set_type(TaskType::ACTOR_CREATION_TASK);
    spec->set_job_id(JobID::FromInt(1202).Binary());
    spec->set_task_id(TaskID::FromRandom(JobID::FromBinary(spec->job_id())).Binary());
    spec->set_parent_task_id(TaskID::FromRandom(JobID::FromInt(1220)).Binary());
    spec->add_dynamic_return_ids(ObjectID::FromRandom().Binary());
    spec->set_num_returns(1);
    auto creation_spec = spec->mutable_actor_creation_task_spec();
    creation_spec->set_actor_id(ActorID::Of(JobID::FromBinary(spec->job_id()),
                                            TaskID::FromBinary(spec->parent_task_id()),
                                            0)
                                    .Binary());
    creation_spec->set_ray_namespace("test_namespace");

    // Set the expectations for some side effects.
    EXPECT_CALL(*gcs_actor_manager_, PollOwnerForActorOutOfScope(_));
    MockGcsNodeTable node_table;
    MockGcsActorTaskSpecTable spec_table;
    EXPECT_CALL(*gcs_table_storage_, ActorTaskSpecTable())
        .WillOnce(ReturnRef(spec_table));
    EXPECT_CALL(spec_table, Put(_, _, _)).WillOnce(Return(Status::OK()));

    gcs_actor_manager_->HandleRegisterActor(request, &reply, nullptr);
  }
};

MATCHER(IsPreemptedActor, "") {
  if (!arg.death_cause().has_actor_died_error_context()) {
    return false;
  }
  return arg.death_cause().actor_died_error_context().preempted();
}

MATCHER(IsPreemptedNode, "") {
  if (arg.death_info().reason() != rpc::NodeDeathInfo::AUTOSCALER_DRAIN) {
    return false;
  }
  return arg.death_info().drain_reason() ==
         rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION;
}

TEST_F(GcsStateTest, TestDrainFailure) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);
  const NodeID node_id = NodeID::FromBinary(node->node_id());
  auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
  ASSERT_TRUE(maybe_node_info.has_value());
  ASSERT_EQ(maybe_node_info.value()->state(), rpc::GcsNodeInfo::ALIVE);

  EXPECT_CALL(*raylet_client_, DrainRaylet)
      .WillOnce(Invoke(
          [this, node_id](const rpc::autoscaler::DrainNodeReason &reason,
                          const std::string &reason_message,
                          const rpc::ClientCallback<rpc::DrainRayletReply> &callback) {
            auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
            // Check node alive.
            ASSERT_TRUE(maybe_node_info.has_value());
            ASSERT_EQ(maybe_node_info.value()->state(), rpc::GcsNodeInfo::ALIVE);
            rpc::DrainRayletReply reply;
            reply.set_is_accepted(false);
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
    ASSERT_EQ(node_info->death_info().reason(), rpc::NodeDeathInfo::UNSPECIFIED);
  };
  gcs_autoscaler_state_manager_->HandleDrainNode(request, &reply, send_reply_callback);
  ASSERT_FALSE(reply.is_accepted());
}

TEST_F(GcsStateTest, TestDrainNode) {
  AddActor("test_actor");
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);
  const NodeID node_id = NodeID::FromBinary(node->node_id());
  auto maybe_node_info = gcs_node_manager_->GetAliveNode(node_id);
  ASSERT_TRUE(maybe_node_info.has_value());
  ASSERT_EQ(maybe_node_info.value()->state(), rpc::GcsNodeInfo::ALIVE);

  EXPECT_CALL(*raylet_client_, DrainRaylet)
      .WillOnce(Invoke(
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
        const auto node_ip_address = node->node_manager_address();
        gcs_actor_manager_->OnNodeDead(node, node_ip_address);
      });

  ON_CALL(*gcs_publisher_, PublishError).WillByDefault(Return(Status::OK()));

  // Set the expectation that we will get a preempted node. This is the actual test!
  MockGcsNodeTable node_table;
  EXPECT_CALL(*gcs_table_storage_, NodeTable()).WillOnce(ReturnRef(node_table));
  EXPECT_CALL(node_table, Put(node_id, IsPreemptedNode(), _))
      .WillOnce(Return(Status::OK()));

  // Simulate raylet failure
  gcs_node_manager_->OnNodeFailure(node_id, [](Status) {});
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
