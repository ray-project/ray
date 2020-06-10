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

#include <ray/gcs/gcs_server/test/gcs_server_test_util.h>
#include <ray/gcs/test/gcs_test_util.h>

#include <memory>
#include "gtest/gtest.h"

namespace ray {

class GcsPlacementGroupSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    worker_client_ = std::make_shared<GcsServerMocker::MockWorkerClient>();
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, error_info_accessor_, gcs_pub_sub_, gcs_table_storage_);
    gcs_placement_group_scheduler_ = std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
        io_service_, gcs_table_storage_, *gcs_node_manager_, gcs_pub_sub_,
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          failure_placement_groups_.emplace_back(std::move(placement_group));
        },
        /*schedule_success_handler=*/
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          success_placement_groups_.emplace_back(std::move(placement_group));
        }
        /*client_factory=*/
        // [this](const rpc::Address &address) { return worker_client_; });
    );
  }

 protected:
  boost::asio::io_service io_service_;
  GcsServerMocker::MockedPlacementGroupInfoAccessor placement_group_info_accessor_;
  GcsServerMocker::MockedNodeInfoAccessor node_info_accessor_;
  GcsServerMocker::MockedErrorInfoAccessor error_info_accessor_;

  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler> gcs_placement_group_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> success_placement_groups_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> failure_placement_groups_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<GcsServerMocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

TEST_F(GcsPlacementGroupSchedulerTest, TestScheduleFailedWithZeroNode) {
ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest(job_id);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with zero node.
  gcs_placement_group_scheduler_->Schedule(placement_group);

  // The lease request should not be send and the scheduling of placement_group should fail as there
  // are no available nodes.
  ASSERT_EQ(0, success_placement_groups_.size());
  ASSERT_EQ(1, failure_placement_groups_.size());
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
} 

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
