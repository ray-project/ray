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
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_common.h"
#include "ray/util/container_util.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
}

namespace ray {
inline bool operator==(const ray::rpc::SchedulingStrategy &lhs,
                       const ray::rpc::SchedulingStrategy &rhs) {
  if (lhs.scheduling_strategy_case() != rhs.scheduling_strategy_case()) {
    return false;
  }

  switch (lhs.scheduling_strategy_case()) {
  case ray::rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy: {
    return (lhs.node_affinity_scheduling_strategy().node_id() ==
            rhs.node_affinity_scheduling_strategy().node_id()) &&
           (lhs.node_affinity_scheduling_strategy().soft() ==
            rhs.node_affinity_scheduling_strategy().soft());
  }
  case ray::rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy: {
    return (lhs.placement_group_scheduling_strategy().placement_group_id() ==
            rhs.placement_group_scheduling_strategy().placement_group_id()) &&
           (lhs.placement_group_scheduling_strategy().placement_group_bundle_index() ==
            rhs.placement_group_scheduling_strategy().placement_group_bundle_index()) &&
           (lhs.placement_group_scheduling_strategy()
                .placement_group_capture_child_tasks() ==
            rhs.placement_group_scheduling_strategy()
                .placement_group_capture_child_tasks());
  }
  default:
    return true;
  }
}

typedef int SchedulingClass;

struct SchedulingClassDescriptor {
 public:
  explicit SchedulingClassDescriptor(ResourceSet rs,
                                     FunctionDescriptor fd,
                                     int64_t d,
                                     rpc::SchedulingStrategy scheduling_strategy)
      : resource_set(std::move(rs)),
        function_descriptor(std::move(fd)),
        depth(d),
        scheduling_strategy(std::move(scheduling_strategy)) {}
  ResourceSet resource_set;
  FunctionDescriptor function_descriptor;
  int64_t depth;
  rpc::SchedulingStrategy scheduling_strategy;

  bool operator==(const SchedulingClassDescriptor &other) const {
    return depth == other.depth && resource_set == other.resource_set &&
           function_descriptor == other.function_descriptor &&
           scheduling_strategy == other.scheduling_strategy;
  }

  std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{"
           << "depth=" << depth << " "
           << "function_descriptor=" << function_descriptor->ToString() << " "
           << "scheduling_strategy=" << scheduling_strategy.DebugString() << " "
           << "resource_set="
           << "{";
    for (const auto &pair : resource_set.GetResourceMap()) {
      buffer << pair.first << " : " << pair.second << ", ";
    }
    buffer << "}}";
    return buffer.str();
  }
};
}  // namespace ray

namespace std {
template <>
struct hash<ray::rpc::SchedulingStrategy> {
  size_t operator()(const ray::rpc::SchedulingStrategy &scheduling_strategy) const {
    size_t hash = std::hash<size_t>()(scheduling_strategy.scheduling_strategy_case());
    if (scheduling_strategy.scheduling_strategy_case() ==
        ray::rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy) {
      hash ^= std::hash<std::string>()(
          scheduling_strategy.node_affinity_scheduling_strategy().node_id());
      // soft returns a bool
      hash ^= static_cast<size_t>(
          scheduling_strategy.node_affinity_scheduling_strategy().soft());
    } else if (scheduling_strategy.scheduling_strategy_case() ==
               ray::rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy) {
      hash ^= std::hash<std::string>()(
          scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
      hash ^= scheduling_strategy.placement_group_scheduling_strategy()
                  .placement_group_bundle_index();
      // placement_group_capture_child_tasks returns a bool
      hash ^=
          static_cast<size_t>(scheduling_strategy.placement_group_scheduling_strategy()
                                  .placement_group_capture_child_tasks());
    }
    return hash;
  }
};

template <>
struct hash<ray::SchedulingClassDescriptor> {
  size_t operator()(const ray::SchedulingClassDescriptor &sched_cls) const {
    size_t hash = std::hash<ray::ResourceSet>()(sched_cls.resource_set);
    hash ^= sched_cls.function_descriptor->Hash();
    hash ^= sched_cls.depth;
    hash ^= std::hash<ray::rpc::SchedulingStrategy>()(sched_cls.scheduling_strategy);
    return hash;
  }
};
}  // namespace std

namespace ray {

/// ConcurrencyGroup is a group of actor methods that shares
/// a executing thread pool.
struct ConcurrencyGroup {
  // Name of this group.
  std::string name;
  // Max concurrency of this group.
  uint32_t max_concurrency;
  // Function descriptors of the actor methods in this group.
  std::vector<ray::FunctionDescriptor> function_descriptors;

  ConcurrencyGroup() = default;

  ConcurrencyGroup(const std::string &name,
                   uint32_t max_concurrency,
                   const std::vector<ray::FunctionDescriptor> &fds)
      : name(name), max_concurrency(max_concurrency), function_descriptors(fds) {}

  std::string GetName() const { return name; }

  uint32_t GetMaxConcurrency() const { return max_concurrency; }

  std::vector<ray::FunctionDescriptor> GetFunctionDescriptors() const {
    return function_descriptors;
  }
};

static inline rpc::ObjectReference GetReferenceForActorDummyObject(
    const ObjectID &object_id) {
  rpc::ObjectReference ref;
  ref.set_object_id(object_id.Binary());
  return ref;
};

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
  explicit TaskSpecification(rpc::TaskSpec &&message)
      : MessageWrapper(std::move(message)) {
    ComputeResources();
  }

  explicit TaskSpecification(const rpc::TaskSpec &message) : MessageWrapper(message) {
    ComputeResources();
  }

  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit TaskSpecification(std::shared_ptr<rpc::TaskSpec> message)
      : MessageWrapper(message) {
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

  JobID JobId() const;

  TaskID ParentTaskId() const;

  size_t ParentCounter() const;

  ray::FunctionDescriptor FunctionDescriptor() const;

  [[nodiscard]] rpc::RuntimeEnvInfo RuntimeEnvInfo() const;

  std::string SerializedRuntimeEnv() const;

  rpc::RuntimeEnvConfig RuntimeEnvConfig() const;

  bool HasRuntimeEnv() const;

  int GetRuntimeEnvHash() const;

  uint64_t AttemptNumber() const;

  bool IsRetry() const;

  int32_t MaxRetries() const;

  size_t NumArgs() const;

  size_t NumReturns() const;

  bool ArgByRef(size_t arg_index) const;

  ObjectID ArgId(size_t arg_index) const;

  const rpc::ObjectReference &ArgRef(size_t arg_index) const;

  ObjectID ReturnId(size_t return_index) const;

  bool ReturnsDynamic() const;

  std::vector<ObjectID> DynamicReturnIds() const;

  void AddDynamicReturnId(const ObjectID &dynamic_return_id);

  const uint8_t *ArgData(size_t arg_index) const;

  size_t ArgDataSize(size_t arg_index) const;

  const uint8_t *ArgMetadata(size_t arg_index) const;

  size_t ArgMetadataSize(size_t arg_index) const;

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

  // Methods specific to actor tasks.

  ActorID ActorId() const;

  TaskID CallerId() const;

  const std::string GetSerializedActorHandle() const;

  const rpc::Address &CallerAddress() const;

  WorkerID CallerWorkerId() const;

  uint64_t ActorCounter() const;

  ObjectID ActorCreationDummyObjectId() const;

  int MaxActorConcurrency() const;

  bool IsAsyncioActor() const;

  bool IsDetachedActor() const;

  ObjectID ActorDummyObject() const;

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

  std::string ConcurrencyGroupName() const;

  bool ExecuteOutOfOrder() const;

  bool IsSpreadSchedulingStrategy() const;

  /// \return true if the task or actor is retriable.
  bool IsRetriable() const;

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

  /// Below static fields could be mutated in `ComputeResources` concurrently due to
  /// multi-threading, we need a mutex to protect it.
  static absl::Mutex mutex_;
  /// Keep global static id mappings for SchedulingClass for performance.
  static absl::flat_hash_map<SchedulingClassDescriptor, SchedulingClass> sched_cls_to_id_
      GUARDED_BY(mutex_);
  static absl::flat_hash_map<SchedulingClass, SchedulingClassDescriptor> sched_id_to_cls_
      GUARDED_BY(mutex_);
  static int next_sched_id_ GUARDED_BY(mutex_);
};

/// \class WorkerCacheKey
///
/// Class used to cache workers, keyed by runtime_env.
class WorkerCacheKey {
 public:
  /// Create a cache key with the given environment variable overrides and serialized
  /// runtime_env.
  ///
  /// worker. \param serialized_runtime_env The JSON-serialized runtime env for this
  /// worker. \param required_resources The required resouce.
  /// worker. \param is_actor Whether the worker will be an actor. This is set when
  ///         task type isolation between workers is enabled.
  /// worker. \param iis_gpu Whether the worker will be using GPUs. This is set when
  ///         resource type isolation between workers is enabled.
  WorkerCacheKey(const std::string serialized_runtime_env,
                 const absl::flat_hash_map<std::string, double> &required_resources,
                 bool is_actor,
                 bool is_gpu);

  bool operator==(const WorkerCacheKey &k) const;

  /// Check if this worker's environment is empty (the default).
  ///
  /// \return true if there are no environment variables set and the runtime env is the
  /// empty string (protobuf default) or a JSON-serialized empty dict.
  bool EnvIsEmpty() const;

  /// Get the hash for this worker's environment.
  ///
  /// \return The hash of the serialized runtime_env.
  std::size_t Hash() const;

  /// Get the int-valued hash for this worker's environment, useful for portability in
  /// flatbuffers.
  ///
  /// \return The hash truncated to an int.
  int IntHash() const;

 private:
  /// The JSON-serialized runtime env for this worker.
  const std::string serialized_runtime_env;
  /// The required resources for this worker.
  const absl::flat_hash_map<std::string, double> required_resources;
  /// Whether the worker is for an actor.
  const bool is_actor;
  /// Whether the worker is to use a GPU.
  const bool is_gpu;
  /// The cached hash of the worker's environment.  This is set to 0
  /// for unspecified or empty environments.
  mutable std::size_t hash_ = 0;
};

}  // namespace ray
