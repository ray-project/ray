// Copyright 2019-2021 The Ray Authors.
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

#include <google/protobuf/util/message_differencer.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/function_descriptor.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_class_util.h"
#include "ray/common/task/task_common.h"
#include "ray/observability/metric_interface.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
}

namespace ray {

/// ConcurrencyGroup is a group of actor methods that shares
/// a executing thread pool.
struct ConcurrencyGroup {
  // Name of this group.
  std::string name_;
  // Max concurrency of this group.
  uint32_t max_concurrency_;
  // Function descriptors of the actor methods in this group.
  std::vector<ray::FunctionDescriptor> function_descriptors_;

  ConcurrencyGroup() = default;

  ConcurrencyGroup(std::string name,
                   uint32_t max_concurrency,
                   std::vector<ray::FunctionDescriptor> fds)
      : name_(std::move(name)),
        max_concurrency_(max_concurrency),
        function_descriptors_(std::move(fds)) {}

  std::string GetName() const { return name_; }

  uint32_t GetMaxConcurrency() const { return max_concurrency_; }

  std::vector<ray::FunctionDescriptor> GetFunctionDescriptors() const {
    return function_descriptors_;
  }
};

static inline rpc::ObjectReference GetReferenceForActorDummyObject(
    const ObjectID &object_id) {
  rpc::ObjectReference ref;
  ref.set_object_id(object_id.Binary());
  return ref;
};

/// Task attempt is a task with a specific attempt number.
using TaskAttempt = std::pair<TaskID, int32_t>;

/// Wrapper class of protobuf `TaskSpec`, see `common.proto` for details.
/// TODO(ekl) we should consider passing around std::unique_ptr<TaskSpecification>
/// instead `const TaskSpecification`, since this class is actually mutable.
class TaskSpecification : public MessageWrapper<rpc::TaskSpec> {
 public:
  /// Construct an empty task specification. This should not be used directly.
  TaskSpecification() { ComputeResources(); }

  /// Construct from a protobuf message object.
  /// The input message will be copied/moved into this object.
  ///
  /// \param message The protobuf message.
  explicit TaskSpecification(rpc::TaskSpec message) : MessageWrapper(std::move(message)) {
    ComputeResources();
  }

  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit TaskSpecification(std::shared_ptr<rpc::TaskSpec> message)
      : MessageWrapper(std::move(message)) {
    ComputeResources();
  }

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit TaskSpecification(const std::string &serialized_binary)
      : MessageWrapper(serialized_binary) {
    ComputeResources();
  }

  // TODO(swang): Finalize and document these methods.
  TaskID TaskId() const;

  // Get the task id in binary format.
  std::string TaskIdBinary() const;

  JobID JobId() const;

  const rpc::JobConfig &JobConfig() const;

  TaskID ParentTaskId() const;

  // Get the parent task id in binary format.
  std::string ParentTaskIdBinary() const;

  ActorID RootDetachedActorId() const;

  TaskID SubmitterTaskId() const;

  size_t ParentCounter() const;

  ray::FunctionDescriptor FunctionDescriptor() const;

  [[nodiscard]] const rpc::RuntimeEnvInfo &RuntimeEnvInfo() const;

  const std::string &SerializedRuntimeEnv() const;

  const rpc::RuntimeEnvConfig &RuntimeEnvConfig() const;

  bool HasRuntimeEnv() const;

  int GetRuntimeEnvHash() const;

  int32_t AttemptNumber() const;

  bool IsRetry() const;

  int32_t MaxRetries() const;

  size_t NumArgs() const;

  size_t NumReturns() const;

  size_t NumStreamingGeneratorReturns() const;

  ObjectID StreamingGeneratorReturnId(size_t generator_index) const;

  void SetNumStreamingGeneratorReturns(uint64_t num_streaming_generator_returns);

  /// Return true if the argument is passed by reference.
  bool ArgByRef(size_t arg_index) const;

  /// Get the ID of the argument at the given index.
  ///
  /// \param arg_index The index of the argument.
  /// \return The ID of the argument.
  ObjectID ArgObjectId(size_t arg_index) const;

  /// Get the raw object ID of the argument at the given index.
  ///
  /// \param arg_index The index of the argument.
  /// \return The raw object ID string of the argument.
  std::string ArgObjectIdBinary(size_t arg_index) const;

  /// Get the reference of the argument at the given index.
  ///
  /// \param arg_index The index of the argument.
  /// \return The reference of the argument.
  const rpc::ObjectReference &ArgRef(size_t arg_index) const;

  /// Get the tensor transport of the argument at the given index.
  ///
  /// \param arg_index The index of the argument.
  /// \return The tensor transport used to transfer the argument to the task
  /// executor.
  rpc::TensorTransport ArgTensorTransport(size_t arg_index) const;

  ObjectID ReturnId(size_t return_index) const;

  bool ReturnsDynamic() const;

  bool IsStreamingGenerator() const;

  int64_t GeneratorBackpressureNumObjects() const;

  std::vector<ObjectID> DynamicReturnIds() const;

  void AddDynamicReturnId(const ObjectID &dynamic_return_id);

  const uint8_t *ArgData(size_t arg_index) const;

  size_t ArgDataSize(size_t arg_index) const;

  const uint8_t *ArgMetadata(size_t arg_index) const;

  size_t ArgMetadataSize(size_t arg_index) const;

  /// Return true if the task should be retried upon exceptions.
  bool ShouldRetryExceptions() const;

  /// Return the ObjectRefs that were inlined in this task argument.
  const std::vector<rpc::ObjectReference> ArgInlinedRefs(size_t arg_index) const;

  /// Return the scheduling class of the task. The scheduler makes a best effort
  /// attempt to fairly dispatch tasks of different classes, preventing
  /// starvation of any single class of task.
  ///
  /// \return The scheduling class used for fair task queueing.
  const SchedulingClass GetSchedulingClass() const;

  /// Return the resources that are to be acquired during the execution of this
  /// task.
  ///
  /// \return The resources that will be acquired during the execution of this
  /// task.
  const ResourceSet &GetRequiredResources() const;

  /// Return the labels that are required for the node to execute
  /// this task on.
  ///
  /// \return The labels that are required for the execution of this task on a node.
  const LabelSelector &GetLabelSelector() const;

  const rpc::SchedulingStrategy &GetSchedulingStrategy() const;

  bool IsNodeAffinitySchedulingStrategy() const;

  NodeID GetNodeAffinitySchedulingStrategyNodeId() const;

  bool GetNodeAffinitySchedulingStrategySoft() const;

  /// Return the resources that are required for a task to be placed on a node.
  /// This will typically be the same as the resources acquired during execution
  /// and will always be a superset of those resources. However, they may
  /// differ, e.g., actor creation tasks may require more resources to be
  /// scheduled on a machine because the actor creation task may require no
  /// resources itself, but subsequent actor methods may require resources, and
  /// so the placement of the actor should take this into account.
  ///
  /// \return The resources that are required to place a task on a node.
  const ResourceSet &GetRequiredPlacementResources() const;

  /// Return the ObjectIDs of any dependencies passed by reference to this
  /// task. This is recomputed each time, so it can be used if the task spec is
  /// mutated.
  ///
  /// \return The recomputed IDs of the dependencies for the task.
  std::vector<ObjectID> GetDependencyIds() const;

  /// Return the dependencies of this task. This is recomputed each time, so it can
  /// be used if the task spec is mutated.
  /// \return The recomputed dependencies for the task.
  std::vector<rpc::ObjectReference> GetDependencies() const;

  std::string GetDebuggerBreakpoint() const;

  /// Return the depth of this task. The depth of a graph, is the number of
  /// `f.remote()` calls from the driver.
  /// \return The depth.
  int64_t GetDepth() const;

  bool IsDriverTask() const;

  Language GetLanguage() const;

  // Returns the task's name.
  const std::string GetName() const;

  /// Whether this task is a normal task.
  bool IsNormalTask() const;

  /// Whether this task is an actor creation task.
  bool IsActorCreationTask() const;

  /// Whether this task is an actor task.
  bool IsActorTask() const;

  // Returns the serialized exception allowlist for this task.
  const std::string GetSerializedRetryExceptionAllowlist() const;

  // Methods specific to actor creation tasks.

  ActorID ActorCreationId() const;

  int64_t MaxActorRestarts() const;

  std::vector<std::string> DynamicWorkerOptions() const;

  std::vector<std::string> DynamicWorkerOptionsOrEmpty() const;

  absl::flat_hash_map<std::string, std::string> GetLabels() const;

  // Methods specific to actor tasks.

  ActorID ActorId() const;

  TaskID CallerId() const;

  const std::string GetSerializedActorHandle() const;

  const rpc::Address &CallerAddress() const;

  WorkerID CallerWorkerId() const;

  std::string CallerWorkerIdBinary() const;

  NodeID CallerNodeId() const;

  uint64_t SequenceNumber() const;

  ObjectID ActorCreationDummyObjectId() const;

  int MaxActorConcurrency() const;

  bool IsAsyncioActor() const;

  bool IsDetachedActor() const;

  std::string DebugString() const;

  // A one-line summary of the runtime environment for the task. May contain sensitive
  // information such as user-specified environment variables.
  std::string RuntimeEnvDebugString() const;

  // A one-word summary of the task func as a call site (e.g., __main__.foo).
  std::string CallSiteString() const;

  // Lookup the resource shape that corresponds to the static key.
  static SchedulingClassDescriptor &GetSchedulingClassDescriptor(SchedulingClass id);

  // Compute a static key that represents the given resource shape.
  static SchedulingClass GetSchedulingClass(const SchedulingClassDescriptor &sched_cls);

  // Placement Group bundle that this task or actor creation is associated with.
  const BundleID PlacementGroupBundleId() const;

  // Whether or not we should capture parent's placement group implicitly.
  bool PlacementGroupCaptureChildTasks() const;

  // Concurrency groups of the actor.
  std::vector<ConcurrencyGroup> ConcurrencyGroups() const;

  const std::string &ConcurrencyGroupName() const;

  bool AllowOutOfOrderExecution() const;

  bool IsSpreadSchedulingStrategy() const;

  /// \return true if the task or actor is retriable.
  bool IsRetriable() const;

  void EmitTaskMetrics(
      ray::observability::MetricInterface &scheduler_placement_time_s_histogram) const;

  /// \return true if task events from this task should be reported.
  bool EnableTaskEvents() const;

  TaskAttempt GetTaskAttempt() const;

  const rpc::TensorTransport TensorTransport() const;

 private:
  void ComputeResources();

  /// Field storing required resources. Initialized in constructor.
  /// TODO(ekl) consider optimizing the representation of ResourceSet for fast copies
  /// instead of keeping shared pointers here.
  std::shared_ptr<ResourceSet> required_resources_;
  /// Field storing required placement resources. Initialized in constructor.
  std::shared_ptr<ResourceSet> required_placement_resources_;
  /// Cached scheduling class of this task.
  SchedulingClass sched_cls_id_ = 0;
  int runtime_env_hash_ = 0;

  // Field storing label selector for scheduling Task on a node. Initialized in constuctor
  // in ComputeResources() call.
  std::shared_ptr<LabelSelector> label_selector_;
};

}  // namespace ray
