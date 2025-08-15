// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/protobuf/common.pb.h"

namespace ray {

// LeaseSpec captures only the subset of TaskSpec used by the raylet for
// leasing, scheduling, dependency resolution, and cancellation. It mirrors the
// names/signatures used at raylet call sites so it can be substituted anywhere
// TaskSpecification is read in raylet.
class LeaseSpec {
 public:
  explicit LeaseSpec(const TaskSpecification &task_spec);

  // Identity/provenance
  LeaseID LeaseId() const { return lease_id_; }
  JobID JobId() const { return job_id_; }

  // Scheduling class and inputs raylet consumes
  const SchedulingClass GetSchedulingClass() const { return sched_cls_id_; }
  const ResourceSet &GetRequiredResources() const { return required_resources_; }
  const ResourceSet &GetRequiredPlacementResources() const {
    return required_placement_resources_;
  }
  const LabelSelector &GetLabelSelector() const { return label_selector_; }
  const rpc::SchedulingStrategy &GetSchedulingStrategy() const {
    return scheduling_strategy_;
  }
  bool IsNodeAffinitySchedulingStrategy() const;
  NodeID GetNodeAffinitySchedulingStrategyNodeId() const;
  bool GetNodeAffinitySchedulingStrategySoft() const;
  int64_t GetDepth() const { return depth_; }

  // Dependencies interface
  std::vector<ObjectID> GetDependencyIds() const;
  std::vector<rpc::ObjectReference> GetDependencies() const { return dependencies_; }

  // Task/actor type helpers used by raylet
  bool IsNormalTask() const { return type_ == rpc::TaskType::NORMAL_TASK; }
  bool IsActorCreationTask() const { return type_ == rpc::TaskType::ACTOR_CREATION_TASK; }
  bool IsActorTask() const { return type_ == rpc::TaskType::ACTOR_TASK; }
  ActorID ActorCreationId() const { return actor_creation_id_; }

  // Ownership and lifetime
  const rpc::Address &CallerAddress() const { return caller_address_; }
  WorkerID CallerWorkerId() const;
  NodeID CallerNodeId() const;
  bool IsDetachedActor() const { return is_detached_actor_; }
  ActorID RootDetachedActorId() const { return root_detached_actor_id_; }

  // Worker pool matching
  int GetRuntimeEnvHash() const { return runtime_env_hash_; }

  // Placement group bundle where applicable
  const BundleID PlacementGroupBundleId() const;

 private:
  // Identity / provenance
  JobID job_id_;
  LeaseID lease_id_;
  rpc::Address caller_address_;

  // Task kind / actor creation
  rpc::TaskType type_ = rpc::TaskType::NORMAL_TASK;
  ActorID actor_creation_id_;
  bool is_detached_actor_ = false;
  ActorID root_detached_actor_id_;

  // Scheduling
  SchedulingClass sched_cls_id_ = 0;
  ResourceSet required_resources_;
  ResourceSet required_placement_resources_;
  rpc::SchedulingStrategy scheduling_strategy_;
  LabelSelector label_selector_;
  int64_t depth_ = 0;

  // Runtime env
  int runtime_env_hash_ = 0;

  // Dependencies
  std::vector<rpc::ObjectReference> dependencies_;
};

}  // namespace ray
