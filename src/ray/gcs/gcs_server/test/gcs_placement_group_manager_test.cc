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

  void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_group_count]() { ++finished_placement_group_count; });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_group_count]() { ++finished_placement_group_count; });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  mock_placement_group_scheduler_->placement_groups.clear();
  ASSERT_EQ(finished_placement_group_count, 0);

  // Check that the placement_group is in state `ALIVE`.
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto create_placement_group_request =
      Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_group_count]() { ++finished_placement_group_count; });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::ALIVE);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name"),
      PlacementGroupID::FromBinary(
          create_placement_group_request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      create_placement_group_request,
      [&finished_placement_group_count]() { ++finished_placement_group_count; });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups.back();
  mock_placement_group_scheduler_->placement_groups.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  auto condition = [this]() {
    return (int)mock_placement_group_scheduler_->placement_groups.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
