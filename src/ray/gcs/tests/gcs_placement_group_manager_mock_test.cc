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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "mock/ray/gcs/gcs_node_manager.h"
#include "mock/ray/gcs/gcs_placement_group_scheduler.h"
#include "mock/ray/gcs/gcs_resource_manager.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_placement_group_manager.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/util/clock.h"
#include "ray/util/counter_map.h"

using namespace ::testing;  // NOLINT
using namespace ray;        // NOLINT
using namespace ray::gcs;   // NOLINT
namespace ray {
namespace gcs {

class GcsPlacementGroupManagerMockTest : public Test {
 public:
  GcsPlacementGroupManagerMockTest()
      : cluster_resource_manager_(PeriodicalRunner::Create(io_context_)) {}

  void SetUp() override {
    store_client_ = std::make_shared<MockStoreClient>();
    gcs_table_storage_ = std::make_shared<GcsTableStorage>(store_client_);
    gcs_placement_group_scheduler_ =
        std::make_shared<MockGcsPlacementGroupSchedulerInterface>();
    node_manager_ = std::make_unique<MockGcsNodeManager>();
    resource_manager_ = std::make_shared<MockGcsResourceManager>(
        io_context_, cluster_resource_manager_, *node_manager_, NodeID::FromRandom());

    gcs_placement_group_manager_ = std::make_unique<GcsPlacementGroupManager>(
        io_context_,
        gcs_placement_group_scheduler_.get(),
        gcs_table_storage_.get(),
        *resource_manager_,
        [](auto &) { return ""; },
        fake_placement_group_gauge_,
        fake_placement_group_creation_latency_in_ms_histogram_,
        fake_placement_group_scheduling_latency_in_ms_histogram_,
        fake_placement_group_count_gauge_,
        clock_);
    counter_.reset(new CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>());
  }

  FakeClock clock_;
  instrumented_io_context io_context_;
  std::unique_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::shared_ptr<MockGcsPlacementGroupSchedulerInterface> gcs_placement_group_scheduler_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<MockStoreClient> store_client_;
  std::unique_ptr<GcsNodeManager> node_manager_;
  ClusterResourceManager cluster_resource_manager_;
  std::shared_ptr<GcsResourceManager> resource_manager_;
  std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>> counter_;

  // Fake metrics for testing
  ray::observability::FakeGauge fake_placement_group_gauge_;
  ray::observability::FakeHistogram
      fake_placement_group_creation_latency_in_ms_histogram_;
  ray::observability::FakeHistogram
      fake_placement_group_scheduling_latency_in_ms_histogram_;
  ray::observability::FakeGauge fake_placement_group_count_gauge_;
};

TEST_F(GcsPlacementGroupManagerMockTest, PendingQueuePriorityReschedule) {
  auto recovery_req =
      GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 1);
  auto recovery_pg =
      std::make_shared<GcsPlacementGroup>(recovery_req, "", counter_, clock_);
  auto pending_req =
      GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 1);
  auto pending_pg =
      std::make_shared<GcsPlacementGroup>(pending_req, "", counter_, clock_);
  auto cb = [](Status s) {};
  SchedulePgRequest request;
  EXPECT_CALL(*store_client_, AsyncPut(_, _, _, _, _))
      .Times(5)
      .WillRepeatedly(Invoke([](const std::string &,
                                const std::string &,
                                std::string,
                                bool,
                                Postable<void(bool)> callback) {
        std::move(callback).Post("PendingQueuePriorityReschedule", true);
      }));
  EXPECT_CALL(*gcs_placement_group_scheduler_, ScheduleUnplacedBundles(_))
      .Times(4)
      .WillRepeatedly(DoAll(SaveArg<0>(&request)));

  // Create a placement group, then lose its node while a normal PG is pending.
  gcs_placement_group_manager_->RegisterPlacementGroup(recovery_pg, cb);
  io_context_.poll();
  ASSERT_EQ(request.placement_group, recovery_pg);
  const auto dead_node_id = NodeID::FromRandom();
  recovery_pg->GetMutableBundle(0)->set_node_id(dead_node_id.Binary());
  recovery_pg->UpdateState(rpc::PlacementGroupTableData::PREPARED);
  request.success_callback(recovery_pg);
  io_context_.restart();
  io_context_.poll();
  ASSERT_EQ(recovery_pg->GetState(), rpc::PlacementGroupTableData::CREATED);

  gcs_placement_group_manager_->RegisterPlacementGroup(pending_pg, cb);
  EXPECT_CALL(*gcs_placement_group_scheduler_, GetAndRemoveBundlesOnNode(dead_node_id))
      .WillOnce(Return(absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>{
          {recovery_pg->GetPlacementGroupID(), {0}}}));
  gcs_placement_group_manager_->OnNodeDead(dead_node_id);

  // Fresh recovery retains highest priority, ahead of the due normal PG.
  auto &pending_queue = gcs_placement_group_manager_->pending_placement_groups_;
  ASSERT_EQ(2, pending_queue.size());
  ASSERT_EQ(pending_queue.begin()->first, 0);
  ASSERT_EQ(pending_queue.begin()->second.second, recovery_pg);
  ASSERT_EQ(pending_queue.rbegin()->first, clock_.NowUnixNanos());
  ASSERT_EQ(pending_queue.rbegin()->second.second, pending_pg);
  ASSERT_EQ(recovery_pg->GetState(), rpc::PlacementGroupTableData::RESCHEDULING);
  io_context_.restart();
  io_context_.poll();
  ASSERT_EQ(request.placement_group, recovery_pg);
  ASSERT_EQ(recovery_pg->GetStats().scheduling_attempt(), 2);
  ASSERT_EQ(pending_pg->GetStats().scheduling_attempt(), 0);

  // A feasible recovery failure uses the existing retry backoff instead of rank 0.
  const auto now = clock_.NowUnixNanos();
  const auto retry_delay_ns =
      1000000 * RayConfig::instance().gcs_create_placement_group_retry_min_interval_ms();
  request.failure_callback(recovery_pg, /*is_feasible=*/true);
  ASSERT_EQ(2, pending_queue.size());
  ASSERT_EQ(recovery_pg->GetStats().highest_retry_delay_ms(), retry_delay_ns / 1000000);
  const auto retry_rank = pending_queue.rbegin()->first;
  ASSERT_EQ(pending_queue.rbegin()->second.second, recovery_pg);
  ASSERT_GT(retry_rank, now);
  ASSERT_EQ(retry_rank, now + retry_delay_ns);

  // The failure continuation lets the normal PG schedule during recovery's cooldown.
  io_context_.restart();
  io_context_.poll();
  ASSERT_EQ(request.placement_group, pending_pg);
  ASSERT_EQ(pending_pg->GetState(), rpc::PlacementGroupTableData::PENDING);
  ASSERT_EQ(pending_pg->GetStats().scheduling_attempt(), 1);
  pending_pg->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  pending_pg->UpdateState(rpc::PlacementGroupTableData::PREPARED);
  request.success_callback(pending_pg);
  io_context_.restart();
  io_context_.poll();
  ASSERT_EQ(pending_pg->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_EQ(pending_queue.begin()->first, retry_rank);
  ASSERT_EQ(recovery_pg->GetStats().scheduling_attempt(), 2);

  // Recovery retries when due and retains the backoff state across failures.
  clock_.AdvanceTime(absl::Nanoseconds(retry_rank - clock_.NowUnixNanos()));
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_TRUE(pending_queue.empty());
  ASSERT_EQ(request.placement_group, recovery_pg);
  ASSERT_EQ(recovery_pg->GetStats().scheduling_attempt(), 3);
  request.failure_callback(recovery_pg, /*is_feasible=*/true);
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_EQ(pending_queue.begin()->second.second, recovery_pg);
  const auto next_delay_ns = pending_queue.begin()->first - clock_.NowUnixNanos();
  ASSERT_GT(next_delay_ns, retry_delay_ns);
  ASSERT_EQ(next_delay_ns,
            retry_delay_ns *
                RayConfig::instance().gcs_create_placement_group_retry_multiplier());
  ASSERT_EQ(recovery_pg->GetStats().highest_retry_delay_ms(), next_delay_ns / 1000000);
}

TEST_F(GcsPlacementGroupManagerMockTest, PendingQueuePriorityFailed) {
  // Test priority works
  //   When return with a failure, exp backoff should work
  auto req = GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 1);
  auto pg = std::make_shared<GcsPlacementGroup>(req, "", counter_, clock_);
  auto cb = [](Status s) {};
  SchedulePgRequest request;
  std::unique_ptr<Postable<void(bool)>> put_cb;
  EXPECT_CALL(*store_client_, AsyncPut(_, _, _, _, _))
      .WillOnce(DoAll(SaveArgToUniquePtr<4>(&put_cb)));
  EXPECT_CALL(*gcs_placement_group_scheduler_, ScheduleUnplacedBundles(_))
      .Times(2)
      .WillRepeatedly(DoAll(SaveArg<0>(&request)));
  auto now = clock_.NowUnixNanos();
  gcs_placement_group_manager_->RegisterPlacementGroup(pg, cb);
  auto &pending_queue = gcs_placement_group_manager_->pending_placement_groups_;
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_LE(now, pending_queue.begin()->first);
  ASSERT_GE(clock_.NowUnixNanos(), pending_queue.begin()->first);
  std::move(*put_cb).Post("PendingQueuePriorityFailed", true);
  io_context_.poll();
  pg->UpdateState(rpc::PlacementGroupTableData::PENDING);
  now = clock_.NowUnixNanos();
  request.failure_callback(pg, true);
  auto exp_backer = ExponentialBackoff(
      1000000 * RayConfig::instance().gcs_create_placement_group_retry_min_interval_ms(),
      RayConfig::instance().gcs_create_placement_group_retry_multiplier(),
      1000000 * RayConfig::instance().gcs_create_placement_group_retry_max_interval_ms());
  auto next = exp_backer.Next();
  ASSERT_DOUBLE_EQ(
      next,
      1000000 * RayConfig::instance().gcs_create_placement_group_retry_min_interval_ms());
  ASSERT_EQ(1, pending_queue.size());
  auto rank = pending_queue.begin()->first;
  ASSERT_LE(now + next, rank);
  // ScheduleUnplacedBundles is not called here
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_EQ(rank, pending_queue.begin()->first);

  clock_.AdvanceTime(absl::Milliseconds(1) +
                     absl::Nanoseconds(rank - clock_.NowUnixNanos()));
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(0, pending_queue.size());
  pg->UpdateState(rpc::PlacementGroupTableData::PENDING);
  now = clock_.NowUnixNanos();
  request.failure_callback(pg, true);
  next = RayConfig::instance().gcs_create_placement_group_retry_multiplier() * next;
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_LE(now + next, pending_queue.begin()->first);
}

TEST_F(GcsPlacementGroupManagerMockTest, PendingQueuePriorityOrder) {
  // Test priority works
  //   Add two pgs
  //   Fail one and make sure it's scheduled later
  auto req1 = GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 1);
  auto pg1 = std::make_shared<GcsPlacementGroup>(req1, "", counter_, clock_);
  auto req2 = GenCreatePlacementGroupRequest("", rpc::PlacementStrategy::SPREAD, 1);
  auto pg2 = std::make_shared<GcsPlacementGroup>(req2, "", counter_, clock_);
  auto cb = [](Status s) {};
  SchedulePgRequest request;
  std::unique_ptr<Postable<void(bool)>> put_cb;
  EXPECT_CALL(*store_client_, AsyncPut(_, _, _, _, _))
      .Times(2)
      .WillRepeatedly(DoAll(SaveArgToUniquePtr<4>(&put_cb)));
  EXPECT_CALL(*gcs_placement_group_scheduler_, ScheduleUnplacedBundles(_))
      .Times(2)
      .WillRepeatedly(DoAll(SaveArg<0>(&request)));
  gcs_placement_group_manager_->RegisterPlacementGroup(pg1, cb);
  gcs_placement_group_manager_->RegisterPlacementGroup(pg2, cb);
  auto &pending_queue = gcs_placement_group_manager_->pending_placement_groups_;
  ASSERT_EQ(2, pending_queue.size());
  std::move(*put_cb).Post("PendingQueuePriorityOrder", true);
  io_context_.poll();
  ASSERT_EQ(1, pending_queue.size());
  // PG1 is scheduled first, so PG2 is in pending queue
  ASSERT_EQ(pg2, pending_queue.begin()->second.second);
  request.failure_callback(pg1, true);
  ASSERT_EQ(2, pending_queue.size());
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  // PG2 is scheduled for the next, so PG1 is in pending queue
  ASSERT_EQ(1, pending_queue.size());
  ASSERT_EQ(pg1, pending_queue.begin()->second.second);
}

}  // namespace gcs
}  // namespace ray
