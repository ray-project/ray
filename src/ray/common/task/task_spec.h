#pragma once

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

extern "C" {
#include "ray/thirdparty/sha256.h"
}

namespace ray {
typedef ResourceSet SchedulingClassDescriptor;
typedef int SchedulingClass;

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
  TaskSpecification() {}

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

  std::string SerializedRuntimeEnv() const;

  size_t NumArgs() const;

  size_t NumReturns() const;

  bool ArgByRef(size_t arg_index) const;

  ObjectID ArgId(size_t arg_index) const;

  rpc::ObjectReference ArgRef(size_t arg_index) const;

  ObjectID ReturnId(size_t return_index) const;

  const uint8_t *ArgData(size_t arg_index) const;

  size_t ArgDataSize(size_t arg_index) const;

  const uint8_t *ArgMetadata(size_t arg_index) const;

  size_t ArgMetadataSize(size_t arg_index) const;

  /// Return the ObjectIDs that were inlined in this task argument.
  const std::vector<ObjectID> ArgInlinedIds(size_t arg_index) const;

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
  /// \param add_dummy_dependency whether to add a dummy object in the returned objects.
  /// \return The recomputed dependencies for the task.
  std::vector<rpc::ObjectReference> GetDependencies(
      bool add_dummy_dependency = true) const;

  std::string GetDebuggerBreakpoint() const;

  std::unordered_map<std::string, std::string> OverrideEnvironmentVariables() const;

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

  // Methods specific to actor creation tasks.

  ActorID ActorCreationId() const;

  int64_t MaxActorRestarts() const;

  std::vector<std::string> DynamicWorkerOptions() const;

  // Methods specific to actor tasks.

  ActorID ActorId() const;

  TaskID CallerId() const;

  const rpc::Address &CallerAddress() const;

  WorkerID CallerWorkerId() const;

  uint64_t ActorCounter() const;

  ObjectID ActorCreationDummyObjectId() const;

  ObjectID PreviousActorTaskDummyObjectId() const;

  int MaxActorConcurrency() const;

  bool IsAsyncioActor() const;

  bool IsDetachedActor() const;

  ObjectID ActorDummyObject() const;

  std::string DebugString() const;

  // A one-word summary of the task func as a call site (e.g., __main__.foo).
  std::string CallSiteString() const;

  // Lookup the resource shape that corresponds to the static key.
  static SchedulingClassDescriptor &GetSchedulingClassDescriptor(SchedulingClass id);

  // Compute a static key that represents the given resource shape.
  static SchedulingClass GetSchedulingClass(const ResourceSet &sched_cls);

  // Placement Group bundle that this task or actor creation is associated with.
  const BundleID PlacementGroupBundleId() const;

  // Whether or not we should capture parent's placement group implicitly.
  bool PlacementGroupCaptureChildTasks() const;

 private:
  void ComputeResources();

  /// Field storing required resources. Initialized in constructor.
  /// TODO(ekl) consider optimizing the representation of ResourceSet for fast copies
  /// instead of keeping shared pointers here.
  std::shared_ptr<ResourceSet> required_resources_;
  /// Field storing required placement resources. Initialized in constructor.
  std::shared_ptr<ResourceSet> required_placement_resources_;
  /// Cached scheduling class of this task.
  SchedulingClass sched_cls_id_;

  /// Below static fields could be mutated in `ComputeResources` concurrently due to
  /// multi-threading, we need a mutex to protect it.
  static absl::Mutex mutex_;
  /// Keep global static id mappings for SchedulingClass for performance.
  static std::unordered_map<SchedulingClassDescriptor, SchedulingClass> sched_cls_to_id_
      GUARDED_BY(mutex_);
  static std::unordered_map<SchedulingClass, SchedulingClassDescriptor> sched_id_to_cls_
      GUARDED_BY(mutex_);
  static int next_sched_id_ GUARDED_BY(mutex_);
};

}  // namespace ray
