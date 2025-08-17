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

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/types/optional.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

// LeaseSpec captures only the subset of TaskSpec used by the raylet for
// leasing, scheduling, dependency resolution, and cancellation.
class LeaseSpecification : public MessageWrapper<rpc::LeaseSpec> {
 public:
  /// Construct an empty task specification. This should not be used directly.
  LeaseSpecification() { ComputeResources(); }

  explicit LeaseSpecification(rpc::LeaseSpec &&lease_spec);
  explicit LeaseSpecification(const rpc::LeaseSpec &message);
  explicit LeaseSpecification(std::shared_ptr<rpc::LeaseSpec> message);

  // Identity/provenance
  LeaseID LeaseId() const;
  JobID JobId() const;

  // Scheduling class and inputs raylet consumes
  const ResourceSet &GetRequiredResources() const;
  const ResourceSet &GetRequiredPlacementResources() const;
  const LabelSelector &GetLabelSelector() const;
  const rpc::SchedulingStrategy &GetSchedulingStrategy() const;
  bool IsNodeAffinitySchedulingStrategy() const;
  NodeID GetNodeAffinitySchedulingStrategyNodeId() const;
  bool GetNodeAffinitySchedulingStrategySoft() const;

  // Dependencies interface
  std::vector<ObjectID> GetDependencyIds() const;
  std::vector<rpc::ObjectReference> GetDependencies() const;

  // Task/actor type helpers used by raylet
  bool IsNormalTask() const;
  bool IsActorCreationTask() const;
  bool IsActorTask() const;
  ActorID ActorId() const;

  // Ownership and lifetime
  const rpc::Address &CallerAddress() const;
  WorkerID CallerWorkerId() const;
  NodeID CallerNodeId() const;

  // Placement group bundle where applicable
  const BundleID PlacementGroupBundleId() const;

  // Retriable
  bool IsRetriable() const;
  int64_t MaxActorRestarts() const;
  int32_t MaxRetries() const;
  TaskID ParentTaskId() const;
  bool IsDetachedActor() const;
  std::string DebugString() const;

  // Worker pool matching
  int GetRuntimeEnvHash() const;
  Language GetLanguage() const;
  bool HasRuntimeEnv() const;
  const rpc::RuntimeEnvInfo &RuntimeEnvInfo() const;
  const std::string &SerializedRuntimeEnv() const;

  // Additional getters used by raylet
  int64_t GetDepth() const;
  ActorID RootDetachedActorId() const;
  bool IsDriverTask() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  uint64_t AttemptNumber() const;
  bool IsRetry() const;
  std::string GetTaskName() const;
  std::vector<std::string> DynamicWorkerOptionsOrEmpty() const;
  std::vector<std::string> DynamicWorkerOptions() const;
  const rpc::RuntimeEnvConfig &RuntimeEnvConfig() const;
  bool IsSpreadSchedulingStrategy() const;
  SchedulingClass GetSchedulingClass() const;

 private:
  // Helper method to compute cached resources and dependencies
  void ComputeResources();

  SchedulingClass GetSchedulingClass(const SchedulingClassDescriptor &sched_cls);

  // Scheduling
  SchedulingClass sched_cls_id_ = 0;
  std::shared_ptr<ResourceSet> required_resources_;
  std::shared_ptr<ResourceSet> required_placement_resources_;
  std::shared_ptr<LabelSelector> label_selector_;

  // Dependencies
  std::vector<rpc::ObjectReference> dependencies_;

  // Runtime environment hash
  int runtime_env_hash_ = 0;

  /// Below static fields could be mutated in `ComputeResources` concurrently due to
  /// multi-threading, we need a mutex to protect it.
  static absl::Mutex mutex_;
  /// Keep global static id mappings for SchedulingClass for performance.
  static absl::flat_hash_map<SchedulingClassDescriptor, SchedulingClass> sched_cls_to_id_
      ABSL_GUARDED_BY(mutex_);
  static absl::flat_hash_map<SchedulingClass, SchedulingClassDescriptor> sched_id_to_cls_
      ABSL_GUARDED_BY(mutex_);
  static int next_sched_id_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray
