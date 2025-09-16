// Copyright  The Ray Authors.
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

#include <gmock/gmock.h>

#include "ray/gcs/gcs_actor_scheduler.h"

namespace ray {
namespace gcs {

class MockGcsActorSchedulerInterface : public GcsActorSchedulerInterface {
 public:
  MOCK_METHOD(void, Schedule, (std::shared_ptr<GcsActor> actor), (override));
  MOCK_METHOD(void, Reschedule, (std::shared_ptr<GcsActor> actor), (override));
  MOCK_METHOD(std::vector<ActorID>, CancelOnNode, (const NodeID &node_id), (override));
  MOCK_METHOD(void,
              CancelOnLeasing,
              (const NodeID &node_id, const ActorID &actor_id, const LeaseID &lease_id),
              (override));
  MOCK_METHOD(ActorID,
              CancelOnWorker,
              (const NodeID &node_id, const WorkerID &worker_id),
              (override));
  MOCK_METHOD(
      void,
      ReleaseUnusedActorWorkers,
      ((const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers)),
      (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsActorScheduler : public GcsActorScheduler {
 public:
  MockGcsActorScheduler(instrumented_io_context &io_context,
                        GcsActorTable &gcs_actor_table,
                        const GcsNodeManager &gcs_node_manager)
      : GcsActorScheduler(
            io_context,
            gcs_actor_table,
            gcs_node_manager,
            nullptr,
            [](std::shared_ptr<GcsActor>,
               rpc::RequestWorkerLeaseReply::SchedulingFailureType,
               const std::string &) {},
            [](std::shared_ptr<GcsActor>, const rpc::PushTaskReply &) {},
            nullptr) {}

  MOCK_METHOD(void, Schedule, (std::shared_ptr<GcsActor> actor), (override));
  MOCK_METHOD(void, Reschedule, (std::shared_ptr<GcsActor> actor), (override));
  MOCK_METHOD(std::vector<ActorID>, CancelOnNode, (const NodeID &node_id), (override));
  MOCK_METHOD(void,
              CancelOnLeasing,
              (const NodeID &node_id, const ActorID &actor_id, const LeaseID &lease_id),
              (override));
  MOCK_METHOD(ActorID,
              CancelOnWorker,
              (const NodeID &node_id, const WorkerID &worker_id),
              (override));
  MOCK_METHOD(
      void,
      ReleaseUnusedActorWorkers,
      ((const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers)),
      (override));
  MOCK_METHOD(void,
              HandleWorkerLeaseReply,
              (std::shared_ptr<GcsActor> actor,
               std::shared_ptr<rpc::GcsNodeInfo> node,
               const Status &status,
               const rpc::RequestWorkerLeaseReply &reply),
              (override));
  MOCK_METHOD(void,
              RetryLeasingWorkerFromNode,
              (std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node),
              (override));
  MOCK_METHOD(void,
              RetryCreatingActorOnWorker,
              (std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsLeasedWorker> worker),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {}  // namespace gcs
}  // namespace ray
