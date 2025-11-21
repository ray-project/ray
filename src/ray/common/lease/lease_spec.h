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
#include <string>
#include <utility>
#include <vector>

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/fallback_strategy.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_class_util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

// LeaseSpec captures only the subset of TaskSpec used by the raylet for
// leasing, scheduling, dependency resolution, and cancellation.
class LeaseSpecification : public MessageWrapper<rpc::LeaseSpec> {
 public:
  explicit LeaseSpecification(const rpc::TaskSpec &task_spec);

  /// Construct an empty task specification. This should not be used directly.
  LeaseSpecification() { ComputeResources(); }

  explicit LeaseSpecification(rpc::LeaseSpec lease_spec)
      : MessageWrapper(std::move(lease_spec)) {
    ComputeResources();
  }

  explicit LeaseSpecification(std::shared_ptr<rpc::LeaseSpec> message)
      : MessageWrapper(std::move(message)) {
    ComputeResources();
  }

  LeaseID LeaseId() const;
  JobID JobId() const;

  const ResourceSet &GetRequiredResources() const;
  const ResourceSet &GetRequiredPlacementResources() const;
  const LabelSelector &GetLabelSelector() const;
  const std::vector<FallbackOption> &GetFallbackStrategy() const;
  const rpc::SchedulingStrategy &GetSchedulingStrategy() const;
  bool IsNodeAffinitySchedulingStrategy() const;
  NodeID GetNodeAffinitySchedulingStrategyNodeId() const;
  bool GetNodeAffinitySchedulingStrategySoft() const;
  std::vector<ObjectID> GetDependencyIds() const;
  const std::vector<rpc::ObjectReference> &GetDependencies() const;

  bool IsNormalTask() const;
  bool IsActorCreationTask() const;
  ActorID ActorId() const;

  const rpc::Address &CallerAddress() const;
  WorkerID CallerWorkerId() const;
  NodeID CallerNodeId() const;
  BundleID PlacementGroupBundleId() const;
  bool IsRetriable() const;
  TaskID ParentTaskId() const;
  bool IsDetachedActor() const;
  std::string DebugString() const;
  int GetRuntimeEnvHash() const;
  rpc::Language GetLanguage() const;
  bool HasRuntimeEnv() const;
  const rpc::RuntimeEnvInfo &RuntimeEnvInfo() const;
  const std::string &SerializedRuntimeEnv() const;
  int64_t GetDepth() const;
  ActorID RootDetachedActorId() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  int64_t MaxActorRestarts() const;
  int32_t MaxRetries() const;
  int32_t AttemptNumber() const;
  bool IsRetry() const;
  std::string GetTaskName() const;
  std::string GetFunctionOrActorName() const;
  std::vector<std::string> DynamicWorkerOptionsOrEmpty() const;
  std::vector<std::string> DynamicWorkerOptions() const;
  size_t DynamicWorkerOptionsSize() const;
  const rpc::RuntimeEnvConfig &RuntimeEnvConfig() const;
  bool IsSpreadSchedulingStrategy() const;
  SchedulingClass GetSchedulingClass() const;
  const rpc::LeaseSpec &GetMessage() const;

 private:
  void ComputeResources();

  SchedulingClass GetSchedulingClass(const SchedulingClassDescriptor &sched_cls);

  SchedulingClass sched_cls_id_ = 0;
  std::shared_ptr<ResourceSet> required_resources_;
  std::shared_ptr<ResourceSet> required_placement_resources_;
  std::shared_ptr<LabelSelector> label_selector_;
  std::shared_ptr<std::vector<FallbackOption>> fallback_strategy_;

  std::vector<rpc::ObjectReference> dependencies_;

  int runtime_env_hash_ = 0;
};

}  // namespace ray
