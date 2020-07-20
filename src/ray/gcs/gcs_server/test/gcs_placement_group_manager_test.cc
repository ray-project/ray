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

using ::testing::_;

class MockPlacementGroupScheduler : public gcs::GcsPlacementGroupSchedulerInterface {
 public:
  MockPlacementGroupScheduler() {}

  void Schedule(std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)>
                    schedule_failure_handler = nullptr,
                std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)>
                    schedule_success_handler = nullptr) {
    placement_groups.push_back(placement_group);
  }

  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups;
};

class GcsPlacementGroupManagerTest : public ::testing::Test {
 public:
  GcsPlacementGroupManagerTest()
      : mock_placement_group_scheduler_(new MockPlacementGroupScheduler()) {
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_placement_group_manager_.reset(new gcs::GcsPlacementGroupManager(
        io_service_, mock_placement_group_scheduler_, gcs_table_storage_));
  }

  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
};

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> finished_placement_groups;
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_groups](
          std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        finished_placement_groups.emplace_back(placement_group);
      });
  ASSERT_EQ(finished_placement_groups.size(), 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(finished_placement_groups.size(), 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> finished_placement_groups;
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_groups](
          std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        finished_placement_groups.emplace_back(placement_group);
      });

  ASSERT_EQ(finished_placement_groups.size(), 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  mock_placement_group_scheduler_->placement_groups.clear();
  ASSERT_EQ(finished_placement_groups.size(), 0);

  // Check that the placement_group is in state `ALIVE`.
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(finished_placement_groups.size(), 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto create_placement_group_request =
      Mocker::GenCreatePlacementGroupRequest("test_name");
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> finished_placement_groups;
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_groups](
          std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        finished_placement_groups.emplace_back(placement_group);
      });

  ASSERT_EQ(finished_placement_groups.size(), 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(finished_placement_groups.size(), 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name"),
      PlacementGroupID::FromBinary(
          create_placement_group_request.placement_group_spec().placement_group_id()));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
