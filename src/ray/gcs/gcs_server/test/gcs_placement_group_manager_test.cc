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
  MockPlacementGroupScheduler() = default;

  void ScheduleUnplacedBundles(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> failure_handler,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> success_handler)
      override {
    placement_groups_.push_back(placement_group);
  }

  MOCK_METHOD1(DestroyPlacementGroupBundleResourcesIfExists,
               void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(MarkScheduleCancelled, void(const PlacementGroupID &placement_group_id));

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const ClientID &node_id) override {
    absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles;
    bundles[group_on_dead_node_] = bundles_on_dead_node_;
    return bundles;
  }

  PlacementGroupID group_on_dead_node_;
  std::vector<int64_t> bundles_on_dead_node_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups_;
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
    // mock_placement_group_scheduler_.reset(new MockPlacementGroupScheduler());
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
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(finished_placement_group_count, 0);

  // Check that the placement_group is in state `CREATED`.
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name"),
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  auto condition = [this]() {
    return (int)mock_placement_group_scheduler_->placement_groups_.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingPendingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  std::atomic<int> failed_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count, &failed_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        } else {
          ++failed_placement_group_count;
        }
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(failed_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  WaitForExpectedCount(finished_placement_group_count, 0);
  WaitForExpectedCount(failed_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingLeasingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  std::atomic<int> failed_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count, &failed_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        } else {
          ++failed_placement_group_count;
        }
      });

  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(failed_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  WaitForExpectedCount(finished_placement_group_count, 0);
  WaitForExpectedCount(failed_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingCreatedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request),
      [&finished_placement_group_count](Status status) {
        if (status.ok()) {
          ++finished_placement_group_count;
        }
      });
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](Status status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(finished_placement_group_count, 1);

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](Status status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeDead) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> finished_placement_group_count(0);
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request1),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  auto request2 = Mocker::GenCreatePlacementGroupRequest();
  gcs_placement_group_manager_->RegisterPlacementGroup(
      std::make_shared<gcs::GcsPlacementGroup>(request2),
      [&finished_placement_group_count](Status status) {
        ++finished_placement_group_count;
      });
  ASSERT_EQ(finished_placement_group_count, 0);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  placement_group->GetMutableBundle(0)->set_node_id(ClientID::FromRandom().Binary());
  placement_group->GetMutableBundle(1)->set_node_id(ClientID::FromRandom().Binary());
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If a node dies, we will set the bundles above it to be unplaced and reschedule the
  // placement group. The placement group state is set to `RESCHEDULING` and will be
  // scheduled first.
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(ClientID::FromRandom());

  // Trigger scheduling `RESCHEDULING` placement group.
  auto finished_group = std::make_shared<gcs::GcsPlacementGroup>(
      placement_group->GetPlacementGroupTableData());
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(finished_group);
  WaitForExpectedCount(finished_placement_group_count, 1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(ClientID::FromBinary(bundles[0]->GetMutableMessage().node_id()).IsNil());
  EXPECT_FALSE(ClientID::FromBinary(bundles[1]->GetMutableMessage().node_id()).IsNil());

  // If `RESCHEDULING` placement group fails to create, we will schedule it again first.
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  auto condition = [this]() {
    return (int)mock_placement_group_scheduler_->placement_groups_.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
