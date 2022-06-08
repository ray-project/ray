
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

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/ray_syncer.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {

enum class GcsPlacementGroupStatus : int32_t {
  SUCCESS = 0,
  FAILURE = 1,
};

class GcsPlacementGroupSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
    for (int index = 0; index < 3; ++index) {
      raylet_clients_.push_back(std::make_shared<GcsServerMocker::MockRayletClient>());
    }
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    auto local_node_id = NodeID::FromRandom();
    cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        scheduling::NodeID(local_node_id.Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        /*is_local_node_with_raylet=*/false);
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_,
        gcs_table_storage_,
        cluster_resource_scheduler_->GetClusterResourceManager(),
        local_node_id);
    ray_syncer_ = std::make_shared<ray::gcs_syncer::RaySyncer>(
        io_service_, nullptr, *gcs_resource_manager_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    raylet_client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &addr) { return raylet_clients_[addr.port()]; });
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        gcs_publisher_, gcs_table_storage_, raylet_client_pool_);
    scheduler_ = std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
        io_service_,
        gcs_table_storage_,
        *gcs_node_manager_,
        *gcs_resource_manager_,
        *cluster_resource_scheduler_,
        raylet_client_pool_,
        ray_syncer_.get());
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  void WaitPlacementGroupPendingDone(int expected_count,
                                     const GcsPlacementGroupStatus status) {
    auto condition = [this, expected_count, status]() {
      absl::MutexLock lock(&placement_group_requests_mutex_);
      return status == GcsPlacementGroupStatus::SUCCESS
                 ? (int)success_placement_groups_.size() == expected_count
                 : (int)failure_placement_groups_.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  void CheckPlacementGroupSize(int expected_count, const GcsPlacementGroupStatus status) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    if (status == GcsPlacementGroupStatus::SUCCESS) {
      ASSERT_EQ(expected_count, success_placement_groups_.size());
    } else {
      ASSERT_EQ(expected_count, failure_placement_groups_.size());
    }
  }

  void CheckEqWithPlacementGroupFront(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      const GcsPlacementGroupStatus status) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    if (status == GcsPlacementGroupStatus::SUCCESS) {
      ASSERT_EQ(placement_group, success_placement_groups_.front());
    } else {
      ASSERT_EQ(placement_group, failure_placement_groups_.front());
    }
  }

  template <typename Data>
  void WaitPendingDone(const std::list<Data> &data, int expected_count) {
    auto condition = [this, &data, expected_count]() {
      absl::MutexLock lock(&placement_group_requests_mutex_);
      return (int)data.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node, int cpu_num = 10) {
    (*node->mutable_resources_total())["CPU"] = cpu_num;
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
  }

  void ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy strategy) {
    ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");

    // Schedule the placement_group with zero node.
    scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
               bool is_insfeasble) {
          absl::MutexLock lock(&placement_group_requests_mutex_);
          failure_placement_groups_.emplace_back(std::move(placement_group));
        },
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&placement_group_requests_mutex_);
          success_placement_groups_.emplace_back(std::move(placement_group));
        });

    // The lease request should not be send and the scheduling of placement_group should
    // fail as there are no available nodes.
    ASSERT_EQ(raylet_clients_[0]->num_lease_requested, 0);
    CheckPlacementGroupSize(0, GcsPlacementGroupStatus::SUCCESS);
    CheckPlacementGroupSize(1, GcsPlacementGroupStatus::FAILURE);
    CheckEqWithPlacementGroupFront(placement_group, GcsPlacementGroupStatus::FAILURE);
  }

  void CheckResourceUpdateMatch(
      const std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> &placement_groups,
      bool create) {
    auto resource_buffer = ray_syncer_->resources_buffer_proto_;
    if (create) {
      absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, double>> updates,
          pg;
      for (auto placement_group : placement_groups) {
        for (auto bundle : placement_group->GetBundles()) {
          const auto &resources = bundle->GetFormattedResources();
          pg[bundle->NodeId().Binary()].insert(resources.begin(), resources.end());
        }
      }
      for (auto batch : resource_buffer.batch()) {
        updates[batch.change().node_id()].insert(
            batch.change().updated_resources().begin(),
            batch.change().updated_resources().end());
      }
      ASSERT_EQ(updates, pg);
    } else {
      absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>> updates, pg;
      for (auto placement_group : placement_groups) {
        for (auto bundle : placement_group->GetBundles()) {
          const auto &resources = bundle->GetFormattedResources();
          for (auto [key, _] : resources) {
            pg[bundle->NodeId().Binary()].insert(key);
          }
        }
      }
      for (auto batch : resource_buffer.batch()) {
        updates[batch.change().node_id()].insert(
            batch.change().deleted_resources().begin(),
            batch.change().deleted_resources().end());
      }
      ASSERT_EQ(updates, pg);
    }
  }

  void WaitUntilSyncMessage(int n) {
    for (int i = 0; i < 20; ++i) {
      if (ray_syncer_->resources_buffer_proto_.batch().size() == n) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }

  void SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy strategy) {
    auto node = Mocker::GenNodeInfo();
    AddNode(node);
    ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");

    // Schedule the placement_group with 1 available node, and the lease request should be
    // send to the node.
    scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
               bool is_insfeasble) {
          absl::MutexLock lock(&placement_group_requests_mutex_);
          failure_placement_groups_.emplace_back(std::move(placement_group));
        },
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&placement_group_requests_mutex_);
          success_placement_groups_.emplace_back(std::move(placement_group));
        });

    ASSERT_EQ(1, raylet_clients_[0]->num_lease_requested);
    ASSERT_EQ(1, raylet_clients_[0]->lease_callbacks.size());
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
    ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
    WaitPlacementGroupPendingDone(0, GcsPlacementGroupStatus::FAILURE);
    WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
    CheckEqWithPlacementGroupFront(placement_group, GcsPlacementGroupStatus::SUCCESS);
    WaitUntilSyncMessage(2);
    {
      absl::MutexLock lock(&placement_group_requests_mutex_);
      CheckResourceUpdateMatch(success_placement_groups_, true);
    }
  }

  void ReschedulingWhenNodeAddTest(rpc::PlacementStrategy strategy) {
    AddNode(Mocker::GenNodeInfo(0), 1);
    auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                  bool is_insfeasble) {
      absl::MutexLock lock(&placement_group_requests_mutex_);
      failure_placement_groups_.emplace_back(std::move(placement_group));
    };
    auto success_handler =
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&placement_group_requests_mutex_);
          success_placement_groups_.emplace_back(std::move(placement_group));
        };

    // Failed to schedule the placement group, because the node resources is not enough.
    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
    scheduler_->ScheduleUnplacedBundles(
        placement_group, failure_handler, success_handler);
    WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
    CheckPlacementGroupSize(0, GcsPlacementGroupStatus::SUCCESS);

    // A new node is added, and the rescheduling is successful.
    AddNode(Mocker::GenNodeInfo(0), 2);
    scheduler_->ScheduleUnplacedBundles(
        placement_group, failure_handler, success_handler);
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
    ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
    WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
  }

 protected:
  const std::chrono::milliseconds timeout_ms_{6000};
  absl::Mutex placement_group_requests_mutex_;
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;

  std::vector<std::shared_ptr<GcsServerMocker::MockRayletClient>> raylet_clients_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler> scheduler_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> success_placement_groups_
      GUARDED_BY(placement_group_requests_mutex_);
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> failure_placement_groups_
      GUARDED_BY(placement_group_requests_mutex_);
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  std::shared_ptr<ray::gcs_syncer::RaySyncer> ray_syncer_;
};

TEST_F(GcsPlacementGroupSchedulerTest, TestSpreadScheduleFailedWithZeroNode) {
  ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy::SPREAD);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackScheduleFailedWithZeroNode) {
  ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy::PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackScheduleFailedWithZeroNode) {
  ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy::STRICT_PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictSpreadScheduleFailedWithZeroNode) {
  ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy::STRICT_SPREAD);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSpreadSchedulePlacementGroupSuccess) {
  SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy::SPREAD);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackSchedulePlacementGroupSuccess) {
  SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy::PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackSchedulePlacementGroupSuccess) {
  SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy::STRICT_PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupReplyFailure) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
             bool is_insfeasble) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(1, raylet_clients_[0]->num_lease_requested);
  ASSERT_EQ(1, raylet_clients_[0]->lease_callbacks.size());

  // Reply failure, so the placement group scheduling failed.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources(false));

  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
  WaitPlacementGroupPendingDone(0, GcsPlacementGroupStatus::SUCCESS);
  CheckEqWithPlacementGroupFront(placement_group, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSpreadStrategyResourceCheck) {
  auto node = Mocker::GenNodeInfo(0);
  AddNode(node, 2);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 3, 2);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The node resource is not enough, scheduling failed.
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The node resource is not enough, scheduling failed.
  WaitPlacementGroupPendingDone(2, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupReturnResource) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
             bool is_insfeasble) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(1, raylet_clients_[0]->num_lease_requested);
  ASSERT_EQ(1, raylet_clients_[0]->lease_callbacks.size());
  // Failed to create these two bundles.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources(false));
  ASSERT_EQ(0, raylet_clients_[0]->num_return_requested);
  // Reply the placement_group creation request, then the placement_group should be
  // scheduled successfully.
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
  WaitPlacementGroupPendingDone(0, GcsPlacementGroupStatus::SUCCESS);
  CheckEqWithPlacementGroupFront(placement_group, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyBalancedScheduling) {
  AddNode(Mocker::GenNodeInfo(0));
  AddNode(Mocker::GenNodeInfo(1));
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  // Schedule placement group, it will be evenly distributed over the two nodes.
  int node_select_count[2] = {0, 0};
  int node_commit_count[2] = {0, 0};
  int node_index = 0;
  for (int index = 0; index < 10; ++index) {
    auto request =
        Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
    scheduler_->ScheduleUnplacedBundles(
        placement_group, failure_handler, success_handler);

    node_index = !raylet_clients_[0]->lease_callbacks.empty() ? 0 : 1;
    ++node_select_count[node_index];
    node_commit_count[node_index] += 1;
    ASSERT_TRUE(raylet_clients_[node_index]->GrantPrepareBundleResources());
    WaitPendingDone(raylet_clients_[node_index]->commit_callbacks, 1);
    ASSERT_TRUE(raylet_clients_[node_index]->GrantCommitBundleResources());
    auto condition = [this, node_index, node_commit_count]() {
      return raylet_clients_[node_index]->num_commit_requested ==
             node_commit_count[node_index];
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }
  WaitPlacementGroupPendingDone(10, GcsPlacementGroupStatus::SUCCESS);

  ASSERT_EQ(node_select_count[0], 5);
  ASSERT_EQ(node_select_count[1], 5);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyReschedulingWhenNodeAdd) {
  ReschedulingWhenNodeAddTest(rpc::PlacementStrategy::STRICT_PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyResourceCheck) {
  auto node0 = Mocker::GenNodeInfo(0);
  AddNode(node0);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);

  // Node1 has less number of bundles, but it doesn't satisfy the resource
  // requirement. In this case, the bundles should be scheduled on Node0.
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node1, 1);
  auto create_placement_group_request2 =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
  auto placement_group2 =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request2, "");
  scheduler_->ScheduleUnplacedBundles(placement_group2, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(2, GcsPlacementGroupStatus::SUCCESS);
}

TEST_F(GcsPlacementGroupSchedulerTest, DestroyPlacementGroup) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
             bool is_insfeasble) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(0, GcsPlacementGroupStatus::FAILURE);
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  scheduler_->DestroyPlacementGroupBundleResourcesIfExists(placement_group_id);
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  WaitUntilSyncMessage(4);
  {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    CheckResourceUpdateMatch(success_placement_groups_, false);
  }
  // Subsequent destroy request should not do anything.
  scheduler_->DestroyPlacementGroupBundleResourcesIfExists(placement_group_id);
  ASSERT_FALSE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_FALSE(raylet_clients_[0]->GrantCancelResourceReserve());
}

TEST_F(GcsPlacementGroupSchedulerTest, DestroyCancelledPlacementGroup) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
             bool is_insfeasble) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  // Now, cancel the schedule request.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  scheduler_->MarkScheduleCancelled(placement_group_id);
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_TRUE(raylet_clients_[1]->GrantCancelResourceReserve());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, PlacementGroupCancelledDuringCommit) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
             bool is_insfeasble) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&placement_group_requests_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  // Now, cancel the schedule request.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  scheduler_->MarkScheduleCancelled(placement_group_id);
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_TRUE(raylet_clients_[1]->GrantCancelResourceReserve());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackStrategyReschedulingWhenNodeAdd) {
  ReschedulingWhenNodeAddTest(rpc::PlacementStrategy::PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackStrategyLargeBundlesScheduling) {
  AddNode(Mocker::GenNodeInfo(0));
  AddNode(Mocker::GenNodeInfo(1));
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  // Schedule placement group which has large bundles.
  // One node does not have enough resources, so we will divide bundles to two nodes.
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::PACK, 15);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  // Prepared resource is batched!
  ASSERT_TRUE(raylet_clients_[0]->num_lease_requested == 1);
  ASSERT_TRUE(raylet_clients_[1]->num_lease_requested == 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  // Wait until all resources are prepared.
  WaitPendingDone(raylet_clients_[0]->commit_callbacks,
                  raylet_clients_[0]->num_lease_requested);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks,
                  raylet_clients_[1]->num_lease_requested);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictSpreadRescheduleWhenNodeDead) {
  int node_count = 3;
  for (int index = 0; index < node_count; ++index) {
    auto node = Mocker::GenNodeInfo(index);
    AddNode(node);
  }
  ASSERT_EQ(3, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest(
      "pg1", rpc::PlacementStrategy::STRICT_SPREAD);
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group successfully.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // Prepare bundle resources.
  for (int index = 0; index < node_count; ++index) {
    raylet_clients_[index]->GrantPrepareBundleResources();
  }
  auto condition = [this]() {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    return (int)(raylet_clients_[0]->commit_callbacks.size() +
                 raylet_clients_[1]->commit_callbacks.size() +
                 raylet_clients_[2]->commit_callbacks.size()) == 2;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));

  // Filter out the nodes not scheduled by this placement group.
  int node_index_not_scheduled = -1;
  for (int index = 0; index < node_count; ++index) {
    if (raylet_clients_[index]->commit_callbacks.empty()) {
      node_index_not_scheduled = index;
      break;
    }
  }
  RAY_CHECK(node_index_not_scheduled != -1);

  // Commit bundle resources.
  for (int index = 0; index < node_count; ++index) {
    raylet_clients_[index]->GrantCommitBundleResources();
  }
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);

  // One node is dead, reschedule the placement group.
  auto bundle_on_dead_node = placement_group->GetMutableBundle(0);
  bundle_on_dead_node->clear_node_id();
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // Prepare bundle resources.
  for (int index = 0; index < node_count; ++index) {
    raylet_clients_[index]->GrantPrepareBundleResources();
  }

  // Check the placement group scheduling results.
  auto commit_ready = [this, node_index_not_scheduled]() {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    return raylet_clients_[node_index_not_scheduled]->commit_callbacks.size() == 1;
  };
  EXPECT_TRUE(WaitForCondition(commit_ready, timeout_ms_.count()));

  // Commit bundle resources.
  for (int index = 0; index < node_count; ++index) {
    raylet_clients_[index]->GrantCommitBundleResources();
  }
  WaitPlacementGroupPendingDone(2, GcsPlacementGroupStatus::SUCCESS);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictSpreadStrategyResourceCheck) {
  auto node0 = Mocker::GenNodeInfo(0);
  AddNode(node0);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request = Mocker::GenCreatePlacementGroupRequest(
      "", rpc::PlacementStrategy::STRICT_SPREAD, 2, 2);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, "");
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The number of nodes is less than the number of bundles, scheduling failed.
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);

  // Node1 resource is insufficient, scheduling failed.
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node1, 1);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  WaitPlacementGroupPendingDone(2, GcsPlacementGroupStatus::FAILURE);

  // The node2 resource is enough and the scheduling is successful.
  auto node2 = Mocker::GenNodeInfo(2);
  AddNode(node2);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[2]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[2]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[2]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestBundleLocationIndex) {
  gcs::BundleLocationIndex bundle_location_index;
  /// Generate data.
  const auto node1 = NodeID::FromRandom();
  const auto node2 = NodeID::FromRandom();
  rpc::CreatePlacementGroupRequest request_pg1 =
      Mocker::GenCreatePlacementGroupRequest("pg1");
  const auto pg1_id = PlacementGroupID::FromBinary(
      request_pg1.placement_group_spec().placement_group_id());
  const std::shared_ptr<BundleSpecification> bundle_node1_pg1 =
      std::make_shared<BundleSpecification>(
          BundleSpecification(request_pg1.placement_group_spec().bundles(0)));
  const std::shared_ptr<BundleSpecification> bundle_node2_pg1 =
      std::make_shared<BundleSpecification>(
          BundleSpecification(request_pg1.placement_group_spec().bundles(1)));
  std::shared_ptr<BundleLocations> bundle_locations_pg1 =
      std::make_shared<BundleLocations>();
  (*bundle_locations_pg1)
      .emplace(bundle_node1_pg1->BundleId(), std::make_pair(node1, bundle_node1_pg1));
  (*bundle_locations_pg1)
      .emplace(bundle_node2_pg1->BundleId(), std::make_pair(node2, bundle_node2_pg1));

  rpc::CreatePlacementGroupRequest request_pg2 =
      Mocker::GenCreatePlacementGroupRequest("pg2");
  const auto pg2_id = PlacementGroupID::FromBinary(
      request_pg2.placement_group_spec().placement_group_id());
  const std::shared_ptr<BundleSpecification> bundle_node1_pg2 =
      std::make_shared<BundleSpecification>(
          BundleSpecification(request_pg2.placement_group_spec().bundles(0)));
  const std::shared_ptr<BundleSpecification> bundle_node2_pg2 =
      std::make_shared<BundleSpecification>(
          BundleSpecification(request_pg2.placement_group_spec().bundles(1)));
  std::shared_ptr<BundleLocations> bundle_locations_pg2 =
      std::make_shared<BundleLocations>();
  (*bundle_locations_pg2)[bundle_node1_pg2->BundleId()] =
      std::make_pair(node1, bundle_node1_pg2);
  (*bundle_locations_pg2)[bundle_node2_pg2->BundleId()] =
      std::make_pair(node2, bundle_node2_pg2);

  // Test Addition.
  bundle_location_index.AddBundleLocations(pg1_id, bundle_locations_pg1);
  bundle_location_index.AddBundleLocations(pg2_id, bundle_locations_pg2);

  /// Test Get works
  auto bundle_locations = bundle_location_index.GetBundleLocations(pg1_id).value();
  ASSERT_TRUE((*bundle_locations).size() == 2);
  ASSERT_TRUE((*bundle_locations).contains(bundle_node1_pg1->BundleId()));
  ASSERT_TRUE((*bundle_locations).contains(bundle_node2_pg1->BundleId()));
  // Make sure pg2 is not in the bundle locations
  ASSERT_FALSE((*bundle_locations).contains(bundle_node2_pg2->BundleId()));

  auto bundle_locations2 = bundle_location_index.GetBundleLocations(pg2_id).value();
  ASSERT_TRUE((*bundle_locations2).size() == 2);
  ASSERT_TRUE((*bundle_locations2).contains(bundle_node1_pg2->BundleId()));
  ASSERT_TRUE((*bundle_locations2).contains(bundle_node2_pg2->BundleId()));

  auto bundle_on_node1 = bundle_location_index.GetBundleLocationsOnNode(node1).value();
  ASSERT_TRUE((*bundle_on_node1).size() == 2);
  ASSERT_TRUE((*bundle_on_node1).contains(bundle_node1_pg1->BundleId()));
  ASSERT_TRUE((*bundle_on_node1).contains(bundle_node1_pg2->BundleId()));

  auto bundle_on_node2 = bundle_location_index.GetBundleLocationsOnNode(node2).value();
  ASSERT_TRUE((*bundle_on_node2).size() == 2);
  ASSERT_TRUE((*bundle_on_node2).contains(bundle_node2_pg1->BundleId()));
  ASSERT_TRUE((*bundle_on_node2).contains(bundle_node2_pg2->BundleId()));

  /// Test Erase works
  bundle_location_index.Erase(pg1_id);
  ASSERT_FALSE(bundle_location_index.GetBundleLocations(pg1_id).has_value());
  ASSERT_TRUE(bundle_location_index.GetBundleLocations(pg2_id).value()->size() == 2);
  bundle_location_index.Erase(node1);
  ASSERT_FALSE(bundle_location_index.GetBundleLocationsOnNode(node1).has_value());
  ASSERT_TRUE(bundle_location_index.GetBundleLocations(pg2_id).value()->size() == 1);
  ASSERT_TRUE(bundle_location_index.GetBundleLocationsOnNode(node2).value()->size() == 1);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestNodeDeadDuringPreparingResources) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group.
  // One node is dead, so one bundle failed to schedule.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    ASSERT_TRUE(placement_group->GetUnplacedBundles().size() == 2);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  gcs_node_manager_->RemoveNode(NodeID::FromBinary(node1->node_id()));
  // This should fail because the node is dead.
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources(false));
  ASSERT_TRUE(raylet_clients_[0]->commit_callbacks.size() == 0);
  ASSERT_TRUE(raylet_clients_[1]->commit_callbacks.size() == 0);
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest,
       TestNodeDeadDuringPreparingResourcesRaceCondition) {
  // This covers the scnario where the node is dead right after raylet sends a success
  // response.
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group.
  // One node is dead, so one bundle failed to schedule.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    ASSERT_TRUE(placement_group->GetUnplacedBundles().size() == 1);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  gcs_node_manager_->RemoveNode(NodeID::FromBinary(node1->node_id()));
  // If node is dead right after raylet succeds to create a bundle, it will reply that
  // the request has been succeed. In this case, we should just treating like a committed
  // bundle that is just removed.
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  // This won't send a commit request because a node is dead.
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 0);
  ASSERT_FALSE(raylet_clients_[1]->GrantCommitBundleResources());
  // In this case, we treated the placement group creation successful. Instead,
  // we will reschedule them.
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestNodeDeadDuringCommittingResources) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group.
  // One node is dead, so one bundle failed to schedule.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    ASSERT_TRUE(placement_group->GetUnplacedBundles().size() == 2);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  gcs_node_manager_->RemoveNode(NodeID::FromBinary(node1->node_id()));
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  // Commit will fail because the node is dead.
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources(false));
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestNodeDeadDuringRescheduling) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group successfully.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);

  auto bundles_on_node0 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node0->node_id()));
  ASSERT_EQ(1, bundles_on_node0.size());
  auto bundles_on_node1 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node1->node_id()));
  ASSERT_EQ(1, bundles_on_node1.size());
  // All nodes are dead, reschedule the placement group.
  placement_group->GetMutableBundle(0)->clear_node_id();
  placement_group->GetMutableBundle(1)->clear_node_id();
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  // Before prepare requests are done, suppose a node is dead.
  gcs_node_manager_->RemoveNode(NodeID::FromBinary(node1->node_id()));
  // This should fail since the node is dead.
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources(false));
  // Make sure the commit requests are not sent.
  ASSERT_TRUE(raylet_clients_[0]->commit_callbacks.size() == 0);
  ASSERT_TRUE(raylet_clients_[1]->commit_callbacks.size() == 0);

  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPGCancelledDuringReschedulingCommit) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group successfully.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);

  auto bundles_on_node0 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node0->node_id()));
  ASSERT_EQ(1, bundles_on_node0.size());
  auto bundles_on_node1 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node1->node_id()));
  ASSERT_EQ(1, bundles_on_node1.size());
  // All nodes are dead, reschedule the placement group.
  placement_group->GetMutableBundle(0)->clear_node_id();
  placement_group->GetMutableBundle(1)->clear_node_id();
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // Rescheduling happening.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  // Make sure the commit requests are not sent.
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  // Cancel the placement group scheduling before commit requests are granted.
  scheduler_->MarkScheduleCancelled(placement_group->GetPlacementGroupID());
  // After commits are granted the placement group will be removed.
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPGCancelledDuringReschedulingCommitPrepare) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");

  // Schedule the placement group successfully.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
                                bool is_insfeasble) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&placement_group_requests_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 1);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 1);
  ASSERT_TRUE(raylet_clients_[0]->GrantCommitBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantCommitBundleResources());
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);

  auto bundles_on_node0 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node0->node_id()));
  ASSERT_EQ(1, bundles_on_node0.size());
  auto bundles_on_node1 =
      scheduler_->GetBundlesOnNode(NodeID::FromBinary(node1->node_id()));
  ASSERT_EQ(1, bundles_on_node1.size());
  // All nodes are dead, reschedule the placement group.
  placement_group->GetMutableBundle(0)->clear_node_id();
  placement_group->GetMutableBundle(1)->clear_node_id();
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // Rescheduling happening.
  // Cancel the placement group scheduling before prepare requests are granted.
  scheduler_->MarkScheduleCancelled(placement_group->GetPlacementGroupID());
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  // Make sure the commit requests are not sent.
  WaitPendingDone(raylet_clients_[0]->commit_callbacks, 0);
  WaitPendingDone(raylet_clients_[1]->commit_callbacks, 0);
  // Make sure the placement group creation has failed.
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::SUCCESS);
  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestReleaseUnusedBundles) {
  SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy::SPREAD);
  absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> node_to_bundle;
  scheduler_->ReleaseUnusedBundles(node_to_bundle);
  ASSERT_EQ(1, raylet_clients_[0]->num_release_unused_bundles_requested);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestInitialize) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request, "");
  placement_group->GetMutableBundle(0)->set_node_id(node0->node_id());
  placement_group->GetMutableBundle(1)->set_node_id(node1->node_id());

  absl::flat_hash_map<PlacementGroupID, std::vector<std::shared_ptr<BundleSpecification>>>
      group_to_bundles;
  group_to_bundles[placement_group->GetPlacementGroupID()].emplace_back(
      std::make_shared<BundleSpecification>(*placement_group->GetMutableBundle(0)));
  group_to_bundles[placement_group->GetPlacementGroupID()].emplace_back(
      std::make_shared<BundleSpecification>(*placement_group->GetMutableBundle(1)));
  scheduler_->Initialize(group_to_bundles);

  auto bundles = scheduler_->GetBundlesOnNode(NodeID::FromBinary(node0->node_id()));
  ASSERT_EQ(1, bundles.size());
  ASSERT_EQ(1, bundles[placement_group->GetPlacementGroupID()].size());
  ASSERT_EQ(0, bundles[placement_group->GetPlacementGroupID()][0]);

  bundles = scheduler_->GetBundlesOnNode(NodeID::FromBinary(node1->node_id()));
  ASSERT_EQ(1, bundles.size());
  ASSERT_EQ(1, bundles[placement_group->GetPlacementGroupID()].size());
  ASSERT_EQ(1, bundles[placement_group->GetPlacementGroupID()][0]);
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
