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

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

class GcsPlacementGroupSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletResourceClient>();
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, io_service_, error_info_accessor_, gcs_pub_sub_, gcs_table_storage_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_placement_group_scheduler_ =
        std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
            io_service_, gcs_table_storage_, *gcs_node_manager_,
            /*lease_client_fplacement_groupy=*/
            [this](const rpc::Address &address) { return raylet_client_; });
  }

 protected:
  boost::asio::io_service io_service_;
  GcsServerMocker::MockedErrorInfoAccessor error_info_accessor_;
  std::shared_ptr<gcs::StoreClient> store_client_;

  std::shared_ptr<GcsServerMocker::MockRayletResourceClient> raylet_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler>
      gcs_placement_group_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> success_placement_groups_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> failure_placement_groups_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsPlacementGroupSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with zero node.
  gcs_placement_group_scheduler_->Schedule(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  // The lease request should not be send and the scheduling of placement_group should
  // fail as there are no available nodes.
  ASSERT_EQ(raylet_client_->num_lease_requested, 0);
  ASSERT_EQ(0, success_placement_groups_.size());
  ASSERT_EQ(1, failure_placement_groups_.size());
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupSuccess) {
  auto node = Mocker::GenNodeInfo();
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  gcs_placement_group_scheduler_->Schedule(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(2, raylet_client_->num_lease_requested);
  ASSERT_EQ(2, raylet_client_->lease_callbacks.size());
  ASSERT_TRUE(raylet_client_->GrantResourceReserve());
  ASSERT_TRUE(raylet_client_->GrantResourceReserve());
  ASSERT_EQ(0, failure_placement_groups_.size());
  ASSERT_EQ(1, success_placement_groups_.size());
  ASSERT_EQ(placement_group, success_placement_groups_.front());
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupFailed) {
  auto node = Mocker::GenNodeInfo();
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  gcs_placement_group_scheduler_->Schedule(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(2, raylet_client_->num_lease_requested);
  ASSERT_EQ(2, raylet_client_->lease_callbacks.size());
  ASSERT_TRUE(raylet_client_->GrantResourceReserve(false));
  ASSERT_TRUE(raylet_client_->GrantResourceReserve(false));
  //   // Reply the placement_group creation request, then the placement_group should be
  //   scheduled successfully.
  ASSERT_EQ(1, failure_placement_groups_.size());
  ASSERT_EQ(0, success_placement_groups_.size());
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupReturnResource) {
  auto node = Mocker::GenNodeInfo();
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  gcs_placement_group_scheduler_->Schedule(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(2, raylet_client_->num_lease_requested);
  ASSERT_EQ(2, raylet_client_->lease_callbacks.size());
  // One bundle success and the other failed.
  ASSERT_TRUE(raylet_client_->GrantResourceReserve());
  ASSERT_TRUE(raylet_client_->GrantResourceReserve(false));
  ASSERT_EQ(1, raylet_client_->num_return_requested);
  // Reply the placement_group creation request, then the placement_group should be
  //  scheduled successfully.
  ASSERT_EQ(1, failure_placement_groups_.size());
  ASSERT_EQ(0, success_placement_groups_.size());
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
