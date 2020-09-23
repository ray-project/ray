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
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    for (int index = 0; index < 3; ++index) {
      raylet_clients_.push_back(
          std::make_shared<GcsServerMocker::MockRayletResourceClient>());
    }
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, io_service_, gcs_pub_sub_, gcs_table_storage_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    scheduler_ = std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
        io_service_, gcs_table_storage_, *gcs_node_manager_,
        /*lease_client_fplacement_groupy=*/
        [this](const rpc::Address &address) { return raylet_clients_[address.port()]; });
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  template <typename Data>
  void WaitPendingDone(const std::vector<Data> &data, int expected_count) {
    auto condition = [this, &data, expected_count]() {
      absl::MutexLock lock(&vector_mutex_);
      return (int)data.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node, int cpu_num = 10) {
    gcs_node_manager_->AddNode(node);
    rpc::HeartbeatTableData heartbeat;
    (*heartbeat.mutable_resources_available())["CPU"] = cpu_num;
    gcs_node_manager_->UpdateNodeRealtimeResources(ClientID::FromBinary(node->node_id()),
                                                   heartbeat);
  }

  void ScheduleFailedWithZeroNodeTest(rpc::PlacementStrategy strategy) {
    ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);

    // Schedule the placement_group with zero node.
    scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          failure_placement_groups_.emplace_back(std::move(placement_group));
        },
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          success_placement_groups_.emplace_back(std::move(placement_group));
        });

    // The lease request should not be send and the scheduling of placement_group should
    // fail as there are no available nodes.
    ASSERT_EQ(raylet_clients_[0]->num_lease_requested, 0);
    ASSERT_EQ(0, success_placement_groups_.size());
    ASSERT_EQ(1, failure_placement_groups_.size());
    ASSERT_EQ(placement_group, failure_placement_groups_.front());
  }

  void SchedulePlacementGroupSuccessTest(rpc::PlacementStrategy strategy) {
    auto node = Mocker::GenNodeInfo();
    AddNode(node);
    ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);

    // Schedule the placement_group with 1 available node, and the lease request should be
    // send to the node.
    scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&vector_mutex_);
          failure_placement_groups_.emplace_back(std::move(placement_group));
        },
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&vector_mutex_);
          success_placement_groups_.emplace_back(std::move(placement_group));
        });

    ASSERT_EQ(2, raylet_clients_[0]->num_lease_requested);
    ASSERT_EQ(2, raylet_clients_[0]->lease_callbacks.size());
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    WaitPendingDone(failure_placement_groups_, 0);
    WaitPendingDone(success_placement_groups_, 1);
    ASSERT_EQ(placement_group, success_placement_groups_.front());
  }

  void ReschedulingWhenNodeAddTest(rpc::PlacementStrategy strategy) {
    AddNode(Mocker::GenNodeInfo(0), 1);
    auto failure_handler =
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&vector_mutex_);
          failure_placement_groups_.emplace_back(std::move(placement_group));
        };
    auto success_handler =
        [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
          absl::MutexLock lock(&vector_mutex_);
          success_placement_groups_.emplace_back(std::move(placement_group));
        };

    // Failed to schedule the placement group, because the node resources is not enough.
    auto request = Mocker::GenCreatePlacementGroupRequest("", strategy);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
    scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler,
                                        success_handler);
    WaitPendingDone(failure_placement_groups_, 1);
    ASSERT_EQ(0, success_placement_groups_.size());

    // A new node is added, and the rescheduling is successful.
    AddNode(Mocker::GenNodeInfo(0), 2);
    scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler,
                                        success_handler);
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
    WaitPendingDone(success_placement_groups_, 1);
  }

 protected:
  const std::chrono::milliseconds timeout_ms_{6000};
  absl::Mutex vector_mutex_;
  std::unique_ptr<std::thread> thread_io_service_;
  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;

  std::vector<std::shared_ptr<GcsServerMocker::MockRayletResourceClient>> raylet_clients_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler> scheduler_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> success_placement_groups_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> failure_placement_groups_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
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
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(2, raylet_clients_[0]->num_lease_requested);
  ASSERT_EQ(2, raylet_clients_[0]->lease_callbacks.size());

  // Reply failure, so the placement group scheduling failed.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources(false));
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources(false));
  WaitPendingDone(failure_placement_groups_, 1);
  WaitPendingDone(success_placement_groups_, 0);
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSpreadStrategyResourceCheck) {
  auto node = Mocker::GenNodeInfo(0);
  AddNode(node, 2);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 3, 2);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The node resource is not enough, scheduling failed.
  WaitPendingDone(failure_placement_groups_, 1);

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The node resource is not enough, scheduling failed.
  WaitPendingDone(failure_placement_groups_, 2);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestSchedulePlacementGroupReturnResource) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  ASSERT_EQ(2, raylet_clients_[0]->num_lease_requested);
  ASSERT_EQ(2, raylet_clients_[0]->lease_callbacks.size());
  // One bundle success and the other failed.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources(false));
  ASSERT_EQ(1, raylet_clients_[0]->num_return_requested);
  // Reply the placement_group creation request, then the placement_group should be
  // scheduled successfully.
  WaitPendingDone(failure_placement_groups_, 1);
  WaitPendingDone(success_placement_groups_, 0);
  ASSERT_EQ(placement_group, failure_placement_groups_.front());
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyBalancedScheduling) {
  AddNode(Mocker::GenNodeInfo(0));
  AddNode(Mocker::GenNodeInfo(1));
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  // Schedule placement group, it will be evenly distributed over the two nodes.
  int node_select_count[2] = {0, 0};
  int node_commit_count[2] = {0, 0};
  int node_index = 0;
  for (int index = 0; index < 10; ++index) {
    auto request =
        Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
    auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
    scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler,
                                        success_handler);

    node_index = !raylet_clients_[0]->lease_callbacks.empty() ? 0 : 1;
    ++node_select_count[node_index];
    node_commit_count[node_index] += 2;
    ASSERT_TRUE(raylet_clients_[node_index]->GrantPrepareBundleResources());
    ASSERT_TRUE(raylet_clients_[node_index]->GrantPrepareBundleResources());
    auto condition = [this, node_index, node_commit_count]() {
      return raylet_clients_[node_index]->num_commit_requested ==
             node_commit_count[node_index];
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }
  WaitPendingDone(success_placement_groups_, 10);
  ASSERT_EQ(node_select_count[0], 5);
  ASSERT_EQ(node_select_count[1], 5);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyReschedulingWhenNodeAdd) {
  ReschedulingWhenNodeAddTest(rpc::PlacementStrategy::STRICT_PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictPackStrategyResourceCheck) {
  auto node0 = Mocker::GenNodeInfo(0);
  AddNode(node0);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(success_placement_groups_, 1);

  // Node1 has less number of bundles, but it doesn't satisfy the resource
  // requirement. In this case, the bundles should be scheduled on Node0.
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node1, 1);
  auto create_placement_group_request2 =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::STRICT_PACK);
  auto placement_group2 =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request2);
  scheduler_->ScheduleUnplacedBundles(placement_group2, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(success_placement_groups_, 2);
}

TEST_F(GcsPlacementGroupSchedulerTest, DestroyPlacementGroup) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  WaitPendingDone(failure_placement_groups_, 0);
  WaitPendingDone(success_placement_groups_, 1);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  scheduler_->DestroyPlacementGroupBundleResourcesIfExists(placement_group_id);
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  // Subsequent destroy request should not do anything.
  scheduler_->DestroyPlacementGroupBundleResourcesIfExists(placement_group_id);
  ASSERT_FALSE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_FALSE(raylet_clients_[0]->GrantCancelResourceReserve());
}

TEST_F(GcsPlacementGroupSchedulerTest, DestroyCancelledPlacementGroup) {
  auto node = Mocker::GenNodeInfo();
  AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  // Schedule the placement_group with 1 available node, and the lease request should be
  // send to the node.
  scheduler_->ScheduleUnplacedBundles(
      placement_group,
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        failure_placement_groups_.emplace_back(std::move(placement_group));
      },
      [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
        absl::MutexLock lock(&vector_mutex_);
        success_placement_groups_.emplace_back(std::move(placement_group));
      });

  // Now, cancel the schedule request.
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  scheduler_->MarkScheduleCancelled(placement_group_id);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  ASSERT_TRUE(raylet_clients_[0]->GrantCancelResourceReserve());
  WaitPendingDone(failure_placement_groups_, 1);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackStrategyReschedulingWhenNodeAdd) {
  ReschedulingWhenNodeAddTest(rpc::PlacementStrategy::PACK);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestPackStrategyLargeBundlesScheduling) {
  AddNode(Mocker::GenNodeInfo(0));
  AddNode(Mocker::GenNodeInfo(1));
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  // Schedule placement group which has large bundles.
  // One node does not have enough resources, so we will divide bundles to two nodes.
  auto request =
      Mocker::GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::PACK, 15);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  RAY_CHECK(raylet_clients_[0]->num_lease_requested > 0);
  RAY_CHECK(raylet_clients_[1]->num_lease_requested > 0);
  for (int index = 0; index < raylet_clients_[0]->num_lease_requested; ++index) {
    ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  }
  for (int index = 0; index < raylet_clients_[1]->num_lease_requested; ++index) {
    ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  }
  WaitPendingDone(success_placement_groups_, 1);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestRescheduleWhenNodeDead) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement group successfully.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(success_placement_groups_, 1);

  auto bundles_on_node0 =
      scheduler_->GetBundlesOnNode(ClientID::FromBinary(node0->node_id()));
  ASSERT_EQ(1, bundles_on_node0.size());
  auto bundles_on_node1 =
      scheduler_->GetBundlesOnNode(ClientID::FromBinary(node1->node_id()));
  ASSERT_EQ(1, bundles_on_node1.size());
  // One node is dead, reschedule the placement group.
  auto bundle_on_dead_node = placement_group->GetMutableBundle(0);
  bundle_on_dead_node->clear_node_id();
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  // TODO(ffbin): We need to see which node the other bundles that have been placed are
  // deployed on, and spread them as far as possible. It will be implemented in the next
  // pr.
  raylet_clients_[0]->GrantPrepareBundleResources();
  raylet_clients_[1]->GrantPrepareBundleResources();
  WaitPendingDone(success_placement_groups_, 2);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestStrictSpreadStrategyResourceCheck) {
  auto node0 = Mocker::GenNodeInfo(0);
  AddNode(node0);
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto request = Mocker::GenCreatePlacementGroupRequest(
      "", rpc::PlacementStrategy::STRICT_SPREAD, 2, 2);
  auto placement_group = std::make_shared<gcs::GcsPlacementGroup>(request);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);

  // The number of nodes is less than the number of bundles, scheduling failed.
  WaitPendingDone(failure_placement_groups_, 1);

  // Node1 resource is insufficient, scheduling failed.
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node1, 1);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  WaitPendingDone(failure_placement_groups_, 2);

  // The node2 resource is enough and the scheduling is successful.
  auto node2 = Mocker::GenNodeInfo(2);
  AddNode(node2);
  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[2]->GrantPrepareBundleResources());
  WaitPendingDone(success_placement_groups_, 1);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestBundleLocationIndex) {
  gcs::BundleLocationIndex bundle_location_index;
  /// Generate data.
  const auto node1 = ClientID::FromRandom();
  const auto node2 = ClientID::FromRandom();
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
  std::shared_ptr<gcs::BundleLocations> bundle_locations_pg1 =
      std::make_shared<gcs::BundleLocations>();
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
  std::shared_ptr<gcs::BundleLocations> bundle_locations_pg2 =
      std::make_shared<gcs::BundleLocations>();
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

TEST_F(GcsPlacementGroupSchedulerTest, TestNodeDeadDuringCommitResources) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement group.
  // One node is dead, so one bundle failed to schedule.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    ASSERT_TRUE(placement_group->GetUnplacedBundles().size() == 1);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  gcs_node_manager_->RemoveNode(ClientID::FromBinary(node1->node_id()));
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());
  WaitPendingDone(failure_placement_groups_, 1);
}

TEST_F(GcsPlacementGroupSchedulerTest, TestRemovePlacementGroupDuringCommitResources) {
  auto node0 = Mocker::GenNodeInfo(0);
  auto node1 = Mocker::GenNodeInfo(1);
  AddNode(node0);
  AddNode(node1);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  auto create_placement_group_request = Mocker::GenCreatePlacementGroupRequest();
  auto placement_group =
      std::make_shared<gcs::GcsPlacementGroup>(create_placement_group_request);

  // Schedule the placement group.
  auto failure_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    failure_placement_groups_.emplace_back(std::move(placement_group));
  };
  auto success_handler = [this](std::shared_ptr<gcs::GcsPlacementGroup> placement_group) {
    absl::MutexLock lock(&vector_mutex_);
    success_placement_groups_.emplace_back(std::move(placement_group));
  };

  scheduler_->ScheduleUnplacedBundles(placement_group, failure_handler, success_handler);
  ASSERT_TRUE(raylet_clients_[0]->GrantPrepareBundleResources());
  ASSERT_TRUE(raylet_clients_[1]->GrantPrepareBundleResources());

  // The placement group was removed, so it failed to schedule.
  scheduler_->MarkScheduleCancelled(placement_group->GetPlacementGroupID());
  WaitPendingDone(failure_placement_groups_, 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
