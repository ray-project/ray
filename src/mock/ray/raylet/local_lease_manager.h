// Copyright 2025 The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/raylet/scheduling/local_lease_manager_interface.h"

namespace ray::raylet {
class MockLocalLeaseManager : public LocalLeaseManagerInterface {
 public:
  MOCK_METHOD(void,
              QueueAndScheduleLease,
              (std::shared_ptr<internal::Work> work),
              (override));
  MOCK_METHOD(void, ScheduleAndGrantLeases, (), (override));
  MOCK_METHOD(bool,
              CancelLeases,
              (std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
               rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
               const std::string &scheduling_failure_message),
              (override));
  MOCK_METHOD(std::vector<std::shared_ptr<internal::Work>>,
              CancelLeasesWithoutReply,
              (std::function<bool(const std::shared_ptr<internal::Work> &)> predicate),
              (override));
  MOCK_METHOD((const absl::flat_hash_map<SchedulingClass,
                                         std::deque<std::shared_ptr<internal::Work>>> &),
              GetLeasesToGrant,
              (),
              (const, override));
  MOCK_METHOD((const absl::flat_hash_map<SchedulingClass,
                                         absl::flat_hash_map<WorkerID, int64_t>> &),
              GetBackLogTracker,
              (),
              (const, override));
  MOCK_METHOD(void,
              SetWorkerBacklog,
              (SchedulingClass scheduling_class,
               const WorkerID &worker_id,
               int64_t backlog_size),
              (override));
  MOCK_METHOD(void, ClearWorkerBacklog, (const WorkerID &worker_id), (override));
  MOCK_METHOD(const RayLease *,
              AnyPendingLeasesForResourceAcquisition,
              (int *num_pending_actor_creation, int *num_pending_leases),
              (const, override));
  MOCK_METHOD(void,
              CleanupLease,
              (std::shared_ptr<WorkerInterface> worker, RayLease *lease),
              (override));
  MOCK_METHOD(void, LeasesUnblocked, (const std::vector<LeaseID> &ready_ids), (override));
  MOCK_METHOD(void,
              ReleaseWorkerResources,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReleaseCpuResourcesFromBlockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(bool,
              ReturnCpuResourcesToUnblockedWorker,
              (std::shared_ptr<WorkerInterface> worker),
              (override));
  MOCK_METHOD(ResourceSet, CalcNormalTaskResources, (), (const, override));
  MOCK_METHOD(void, RecordMetrics, (), (const, override));
  MOCK_METHOD(void, DebugStr, (std::stringstream & buffer), (const, override));
  MOCK_METHOD(size_t, GetNumLeaseSpilled, (), (const, override));
  MOCK_METHOD(size_t, GetNumWaitingLeaseSpilled, (), (const, override));
  MOCK_METHOD(size_t, GetNumUnschedulableLeaseSpilled, (), (const, override));
  MOCK_METHOD(bool,
              IsLeaseQueued,
              (const SchedulingClass &scheduling_class, const LeaseID &lease_id),
              (const, override));
  MOCK_METHOD(bool,
              AddReplyCallback,
              (const SchedulingClass &scheduling_class,
               const LeaseID &lease_id,
               rpc::SendReplyCallback send_reply_callback,
               rpc::RequestWorkerLeaseReply *reply),
              (override));
};

}  // namespace ray::raylet
