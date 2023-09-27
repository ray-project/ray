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

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

#include <memory>

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/util/counter_map.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
// clang-format on

namespace ray {
namespace gcs {

using ::testing::_;
using StatusCallback = std::function<void(Status status)>;

class MockPlacementGroupScheduler : public gcs::GcsPlacementGroupSchedulerInterface {
 public:
  MockPlacementGroupScheduler() = default;

  void ScheduleUnplacedBundles(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>, bool)> failure_handler,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> success_handler)
      override {
    absl::MutexLock lock(&mutex_);
    placement_groups_.push_back(placement_group);
  }

  MOCK_METHOD1(DestroyPlacementGroupBundleResourcesIfExists,
               void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(MarkScheduleCancelled, void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(
      ReleaseUnusedBundles,
      void(const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles));

  MOCK_METHOD1(
      Initialize,
      void(const absl::flat_hash_map<PlacementGroupID,
                                     std::vector<std::shared_ptr<BundleSpecification>>>
               &group_to_bundles));

  MOCK_METHOD((absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>),
              GetBundlesOnNode,
              (const NodeID &node_id),
              (const, override));

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetAndRemoveBundlesOnNode(
      const NodeID &node_id) override {
    absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles;
    bundles[group_on_dead_node_] = bundles_on_dead_node_;
    return bundles;
  }

  int GetPlacementGroupCount() {
    absl::MutexLock lock(&mutex_);
    return placement_groups_.size();
  }

  PlacementGroupID group_on_dead_node_;
  std::vector<int64_t> bundles_on_dead_node_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups_;
  absl::Mutex mutex_;
};

class GcsPlacementGroupManagerTest : public ::testing::Test {
 public:
  GcsPlacementGroupManagerTest()
      : mock_placement_group_scheduler_(new MockPlacementGroupScheduler()),
        cluster_resource_manager_(io_service_) {
    gcs_publisher_ =
        std::make_shared<GcsPublisher>(std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_node_manager_ = std::make_shared<gcs::MockGcsNodeManager>();
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, cluster_resource_manager_, *gcs_node_manager_, NodeID::FromRandom());
    gcs_placement_group_manager_.reset(new gcs::GcsPlacementGroupManager(
        io_service_,
        mock_placement_group_scheduler_,
        gcs_table_storage_,
        *gcs_resource_manager_,
        [this](const JobID &job_id) { return job_namespace_table_[job_id]; }));
    counter_.reset(new CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>());
    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
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

  // Make placement group registration sync.
  void RegisterPlacementGroup(const ray::rpc::CreatePlacementGroupRequest &request,
                              StatusCallback callback) {
    std::promise<void> promise;
    JobID job_id = JobID::FromBinary(request.placement_group_spec().creator_job_id());
    std::string ray_namespace = job_namespace_table_[job_id];
    gcs_placement_group_manager_->RegisterPlacementGroup(
        std::make_shared<gcs::GcsPlacementGroup>(request, ray_namespace, counter_),
        [&callback, &promise](Status status) {
          RAY_CHECK_OK(status);
          callback(status);
          promise.set_value();
        });
    promise.get_future().get();
  }

  // We need this to ensure that `MarkSchedulingDone` and `SchedulePendingPlacementGroups`
  // was already invoked when we have invoked `OnPlacementGroupCreationSuccess`.
  void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<gcs::GcsPlacementGroup> &placement_group) {
    std::promise<void> promise;
    gcs_placement_group_manager_->WaitPlacementGroup(
        placement_group->GetPlacementGroupID(), [&promise](Status status) {
          RAY_CHECK_OK(status);
          promise.set_value();
        });

    // mock all bundles of pg have prepared and committed resource.
    int bundles_size = placement_group->GetPlacementGroupTableData().bundles_size();
    for (int bundle_index = 0; bundle_index < bundles_size; bundle_index++) {
      placement_group->GetMutableBundle(bundle_index)
          ->set_node_id(NodeID::FromRandom().Binary());
    }
    gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
    promise.get_future().get();
  }

  std::shared_ptr<GcsInitData> LoadDataFromDataStorage() {
    auto gcs_init_data = std::make_shared<GcsInitData>(gcs_table_storage_);
    std::promise<void> promise;
    gcs_init_data->AsyncLoad([&promise] { promise.set_value(); });
    promise.get_future().get();
    return gcs_init_data;
  }

  void WaitForExpectedPgCount(int expected_count) {
    auto condition = [this, expected_count]() {
      return mock_placement_group_scheduler_->GetPlacementGroupCount() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  }

  void WaitUntilIoServiceDone() {
    // In this test, io service is running in a different thread.
    // That means it can have a thread safety issue, if the test thread
    // touches private attributes while io service is accessing it.
    // This method returns when there's no more task in the io service,
    // meaning it can be used to avoid thread-safety issue.
    // The method should be used after calling APIs that
    // post a new task to IO service.
    auto cnt = 0;
    io_service_.post([&cnt]() { cnt += 1; }, "");
    auto condition = [&cnt]() { return cnt == 1; };
    EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  }

  void StopIoService() { io_service_.stop(); }

  ExponentialBackOff GetExpBackOff() { return ExponentialBackOff(0, 1); }

  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
  absl::flat_hash_map<JobID, std::string> job_namespace_table_;
  std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>> counter_;

 protected:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;
  ClusterResourceManager cluster_resource_manager_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
};

TEST_F(GcsPlacementGroupManagerTest, TestPlacementGroupBundleCache) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  ASSERT_TRUE(placement_group->cached_bundle_specs_.empty());
  // Fill the cache and verify it.
  const auto &bundle_specs = placement_group->GetBundles();
  ASSERT_EQ(placement_group->cached_bundle_specs_, bundle_specs);
  ASSERT_FALSE(placement_group->cached_bundle_specs_.empty());
  // Invalidate the cache and verify it.
  RAY_UNUSED(placement_group->GetMutableBundle(0));
  ASSERT_TRUE(placement_group->cached_bundle_specs_.empty());
}

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 1);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 1);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 1);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);

  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 2);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Check that the placement_group is in state `CREATED`.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRemoveNamedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Remove the named placement group.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group->GetPlacementGroupID(),
      [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
            PlacementGroupID::Nil());
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovedPlacementGroupNotReportedAsLoad) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  /// This is a hack to make a synchronization point here.
  /// This test verifies GetPlacementGroupLoad doesn't return REMOVED pgs.
  /// If IO service keeps running, it will invoke `SchedulePendingPlacementGroups`
  /// which cleans up REMOVED pgs from the pending list (which is
  /// used to obtain the load). It makes test accidently pass when REMOVED
  /// pgs can still be reported as load.
  StopIoService();
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);

  auto load = gcs_placement_group_manager_->GetPlacementGroupLoad();
  ASSERT_EQ(load->placement_group_data_size(), 0);
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);
  WaitForExpectedPgCount(1);
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingPendingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 1);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::REMOVED), 0);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  ASSERT_EQ(placement_group->GetStats().scheduling_state(),
            rpc::PlacementGroupStats::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::REMOVED), 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingLeasingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);

  // Sleep 1 second so that io_service_ can invoke SchedulePendingPlacementGroups.
  // If we invoke it from this thread, then both threads race and cause use-after-free
  // bugs.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Make sure it is not rescheduled
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::REMOVED), 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingCreatedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // We have ensured that this operation is synchronized.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::PENDING), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::CREATED), 0);
  ASSERT_EQ(counter_->Get(rpc::PlacementGroupTableData::REMOVED), 1);
}

TEST_F(GcsPlacementGroupManagerTest, TestReschedulingRetry) {
  ///
  /// Test when the rescheduling is failed, the scheduling is retried.
  /// pg scheduled -> pg created -> node dead ->
  /// pg rescheduled -> rescheduling failed -> retry.
  ///
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  WaitUntilIoServiceDone();

  // Placement group is now rescheduled because bundles are killed.
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());
  WaitUntilIoServiceDone();
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(NodeID::FromBinary(bundles[0]->GetMessage().node_id()).IsNil());
  EXPECT_FALSE(NodeID::FromBinary(bundles[1]->GetMessage().node_id()).IsNil());
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::RESCHEDULING);

  // Rescheduling failed. It should be retried.
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), true);
  WaitUntilIoServiceDone();
  WaitForExpectedPgCount(1);
  // Verify the pg scheduling is retried when its state is RESCHEDULING.
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeDead) {
  ///
  /// Test the basic case.
  /// pg scheduled -> pg created -> node dead -> pg rescheduled.
  ///
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  WaitUntilIoServiceDone();

  // If a node dies, we will set the bundles above it to be unplaced and reschedule the
  // placement group. The placement group state is set to `RESCHEDULING`
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());
  WaitUntilIoServiceDone();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(NodeID::FromBinary(bundles[0]->GetMessage().node_id()).IsNil());
  EXPECT_FALSE(NodeID::FromBinary(bundles[1]->GetMessage().node_id()).IsNil());
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::RESCHEDULING);

  // Test placement group rescheduling success.
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  WaitUntilIoServiceDone();
}

/// TODO(sang): Currently code is badly structured that it is difficult to test
/// the following scenarios. We should rewrite some APIs and handle them.
/// 1. Node is dead before finishing to create a pg
/// (in this case, we should cancel the in-flight scheduling
/// and prioritze rescheduling to avoid partially allocated pg,
/// 2. While doing rescheduling, an additional node is dead.
/// relevant: https://github.com/ray-project/ray/pull/24875

TEST_F(GcsPlacementGroupManagerTest, TestSchedulerReinitializeAfterGcsRestart) {
  // Create a placement group and make sure it has been created successfully.
  auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id);
  auto job_table_data = Mocker::GenJobTableData(job_id);
  RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *job_table_data, nullptr));
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);

  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  placement_group->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  placement_group->GetMutableBundle(1)->set_node_id(NodeID::FromRandom().Binary());
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Reinitialize the placement group manager and test the node dead case.
  auto gcs_init_data = LoadDataFromDataStorage();
  ASSERT_EQ(1, gcs_init_data->PlacementGroups().size());
  EXPECT_TRUE(
      gcs_init_data->PlacementGroups().find(placement_group->GetPlacementGroupID()) !=
      gcs_init_data->PlacementGroups().end());
  EXPECT_CALL(*mock_placement_group_scheduler_, ReleaseUnusedBundles(_)).Times(1);
  EXPECT_CALL(
      *mock_placement_group_scheduler_,
      Initialize(testing::Contains(testing::Key(placement_group->GetPlacementGroupID()))))
      .Times(1);
  gcs_placement_group_manager_->Initialize(*gcs_init_data);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorDeadAndJobDead) {
  // Test the scenario where actor dead -> job dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorAndJobDead) {
  // Test the scenario where job dead -> actor dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenOnlyJobDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest,
       TestAutomaticCleanupDoNothingWhenDifferentJobIsDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  const auto different_job_id = JobID::FromInt(3);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // This shouldn't have been called.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingCanceledWhenPgIsInfeasible) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Mark it non-retryable.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), false);
  ASSERT_EQ(placement_group->GetStats().scheduling_state(),
            rpc::PlacementGroupStats::INFEASIBLE);

  // Schedule twice to make sure it will not be scheduled afterward.
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);

  // Add a node and make sure it will reschedule the infeasible placement group.
  const auto &node_id = NodeID::FromRandom();
  gcs_placement_group_manager_->OnNodeAdd(node_id);

  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(placement_group->GetStats().scheduling_state(),
            rpc::PlacementGroupStats::FINISHED);
}

TEST_F(GcsPlacementGroupManagerTest, TestRayNamespace) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest("test_name");
  job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
  auto request2 = Mocker::GenCreatePlacementGroupRequest(
      "test_name", rpc::PlacementStrategy::SPREAD, 2, 1.0, JobID::FromInt(11));
  auto request3 = Mocker::GenCreatePlacementGroupRequest("test_name");
  {  // Create a placement group in the empty namespace.
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Create a placement group in the empty namespace.
    job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request2, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request2.placement_group_spec().placement_group_id()));
    ASSERT_NE(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Placement groups with the same namespace, different jobs should still collide.
    std::promise<void> promise;
    gcs_placement_group_manager_->RegisterPlacementGroup(
        std::make_shared<gcs::GcsPlacementGroup>(request3, "", counter_),
        [&promise](Status status) {
          ASSERT_FALSE(status.ok());
          promise.set_value();
        });
    promise.get_future().get();

    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
}

TEST_F(GcsPlacementGroupManagerTest, TestStats) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  /// Feasible, but still failing.
  {
    ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 1);
    ASSERT_EQ(placement_group->GetStats().scheduling_state(),
              rpc::PlacementGroupStats::QUEUED);
    gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
        placement_group, GetExpBackOff(), /*is_feasible*/ true);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.clear();
    ASSERT_EQ(placement_group->GetStats().scheduling_state(),
              rpc::PlacementGroupStats::NO_RESOURCES);
    ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 2);
  }

  /// Feasible, but failed to commit resources.
  {
    placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
    gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
        placement_group, GetExpBackOff(), /*is_feasible*/ true);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.clear();
    ASSERT_EQ(placement_group->GetStats().scheduling_state(),
              rpc::PlacementGroupStats::FAILED_TO_COMMIT_RESOURCES);
    ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 3);
  }

  // Check that the placement_group scheduling state is `FINISHED`.
  {
    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetStats().scheduling_state(),
              rpc::PlacementGroupStats::FINISHED);
    ASSERT_EQ(placement_group->GetStats().scheduling_attempt(), 3);
  }
}

TEST_F(GcsPlacementGroupManagerTest, TestStatsCreationTime) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  auto request_received_ns = absl::GetCurrentTimeNanos();
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  /// Failed to create a pg.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(
      placement_group, GetExpBackOff(), /*is_feasible*/ true);
  auto scheduling_started_ns = absl::GetCurrentTimeNanos();
  WaitForExpectedPgCount(1);

  OnPlacementGroupCreationSuccess(placement_group);
  auto scheduling_done_ns = absl::GetCurrentTimeNanos();

  /// Make sure the creation time is correctly recorded.
  ASSERT_TRUE(placement_group->GetStats().scheduling_latency_us() != 0);
  ASSERT_TRUE(placement_group->GetStats().end_to_end_creation_latency_us() != 0);
  // The way to measure latency is a little brittle now. Alternatively, we can mock
  // the absl::GetCurrentNanos() to a callback method and have more accurate test.
  auto scheduling_latency_us =
      absl::Nanoseconds(scheduling_done_ns - scheduling_started_ns) /
      absl::Microseconds(1);
  auto end_to_end_creation_latency_us =
      absl::Nanoseconds(scheduling_done_ns - request_received_ns) / absl::Microseconds(1);
  ASSERT_TRUE(placement_group->GetStats().scheduling_latency_us() <
              scheduling_latency_us);
  ASSERT_TRUE(placement_group->GetStats().end_to_end_creation_latency_us() <
              end_to_end_creation_latency_us);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetAllPlacementGroupInfoLimit) {
  auto num_pgs = 3;
  std::atomic<int> registered_placement_group_count(0);
  for (int i = 0; i < num_pgs; i++) {
    auto request = Mocker::GenCreatePlacementGroupRequest();
    RegisterPlacementGroup(request,
                           [&registered_placement_group_count](const Status &status) {
                             ++registered_placement_group_count;
                           });
  }
  WaitForExpectedPgCount(1);

  {
    rpc::GetAllPlacementGroupRequest request;
    rpc::GetAllPlacementGroupReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    gcs_placement_group_manager_->HandleGetAllPlacementGroup(request, &reply, callback);
    promise.get_future().get();
    ASSERT_EQ(reply.placement_group_table_data().size(), 3);
    ASSERT_EQ(reply.total(), 3);
  }
  {
    rpc::GetAllPlacementGroupRequest request;
    rpc::GetAllPlacementGroupReply reply;
    request.set_limit(2);
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    gcs_placement_group_manager_->HandleGetAllPlacementGroup(request, &reply, callback);
    promise.get_future().get();
    ASSERT_EQ(reply.placement_group_table_data().size(), 2);
    ASSERT_EQ(reply.total(), 3);
  }
}

TEST_F(GcsPlacementGroupManagerTest, TestCheckCreatorJobIsDeadWhenGcsRestart) {
  auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "",
      rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id);
  auto job_table_data = Mocker::GenJobTableData(job_id);
  job_table_data->set_is_dead(true);
  RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *job_table_data, nullptr));

  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);

  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Reinitialize the placement group manager and the job is dead.
  auto gcs_init_data = LoadDataFromDataStorage();
  ASSERT_EQ(1, gcs_init_data->PlacementGroups().size());
  EXPECT_TRUE(
      gcs_init_data->PlacementGroups().find(placement_group->GetPlacementGroupID()) !=
      gcs_init_data->PlacementGroups().end());
  EXPECT_CALL(
      *mock_placement_group_scheduler_,
      Initialize(testing::Contains(testing::Key(placement_group->GetPlacementGroupID()))))
      .Times(1);
  gcs_placement_group_manager_->Initialize(*gcs_init_data);
  // Make sure placement group is removed after gcs restart for the creator job is dead
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
