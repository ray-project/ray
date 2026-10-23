// Copyright The Ray Authors.
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

#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet/metrics.h"
#include "ray/raylet/scheduling/local_lease_manager_interface.h"

namespace ray::raylet {

// Hand-written fake for LocalLeaseManagerInterface. Methods default to no-op /
// trivial returns; reference-returning getters are backed by public members;
// public counters/vectors record calls for tests to assert on.
class FakeLocalLeaseManager : public LocalLeaseManagerInterface {
 public:
  void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) override {
    queue_and_schedule_calls++;
  }

  void ScheduleAndGrantLeases() override {}

  bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) override {
    return false;
  }

  std::vector<std::shared_ptr<internal::Work>> CancelLeasesWithoutReply(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate) override {
    return {};
  }

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetLeasesToGrant() const override {
    return leases_to_grant_;
  }

  const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const override {
    return backlog_tracker_;
  }

  void SetWorkerBacklog(rpc::ReportWorkerBacklogRequest request) override {
    set_worker_backlog_call_count++;
  }

  void ClearWorkerBacklog(const WorkerID &worker_id) override {}

  const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const override {
    return nullptr;
  }

  void CleanupLease(const std::shared_ptr<WorkerInterface> &worker) override {}

  void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) override {}

  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override {}

  bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  void RecordMetrics() const override {}

  SchedulerMetrics &GetSchedulerMetrics() const override { return scheduler_metrics_; }

  void DebugStr(std::stringstream &buffer) const override {}

  size_t GetNumLeaseSpilled() const override { return 0; }

  size_t GetNumWaitingLeaseSpilled() const override { return 0; }

  size_t GetNumUnschedulableLeaseSpilled() const override { return 0; }

  bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                     const LeaseID &lease_id) const override {
    return false;
  }

  bool AddReplyCallback(const SchedulingClass &scheduling_class,
                        const LeaseID &lease_id,
                        rpc::SendReplyCallback send_reply_callback,
                        rpc::RequestWorkerLeaseReply *reply) override {
    return false;
  }

  // Recorded calls / programmable state.
  int queue_and_schedule_calls = 0;
  int set_worker_backlog_call_count = 0;

 private:
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      leases_to_grant_;
  absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      backlog_tracker_;
  // Owned fake metrics that back the SchedulerMetrics references so the fake is
  // default-constructible.
  mutable ray::observability::FakeGauge scheduler_tasks_gauge_;
  mutable ray::observability::FakeGauge scheduler_unscheduleable_tasks_gauge_;
  mutable ray::observability::FakeGauge scheduler_failed_worker_startup_total_gauge_;
  mutable ray::observability::FakeGauge internal_num_spilled_tasks_gauge_;
  mutable ray::observability::FakeGauge internal_num_infeasible_scheduling_classes_gauge_;
  mutable SchedulerMetrics scheduler_metrics_{
      scheduler_tasks_gauge_,
      scheduler_unscheduleable_tasks_gauge_,
      scheduler_failed_worker_startup_total_gauge_,
      internal_num_spilled_tasks_gauge_,
      internal_num_infeasible_scheduling_classes_gauge_};
};

}  // namespace ray::raylet
