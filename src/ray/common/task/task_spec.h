#ifndef RAY_COMMON_TASK_TASK_SPEC_H
#define RAY_COMMON_TASK_TASK_SPEC_H

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_common.h"

extern "C" {
#include "ray/thirdparty/sha256.h"
}

namespace ray {

typedef std::vector<std::string> FunctionDescriptor;
typedef std::pair<ResourceSet, FunctionDescriptor> SchedulingClassDescriptor;
typedef int SchedulingClass;

/// Wrapper class of protobuf `TaskSpec`, see `common.proto` for details.
class TaskSpecification : public MessageWrapper<rpc::TaskSpec> {
 public:
  /// Construct an empty task specification. This should not be used directly.
  TaskSpecification() {}

  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit TaskSpecification(rpc::TaskSpec message) : MessageWrapper(message) {
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

  std::vector<std::string> FunctionDescriptor() const;

  size_t NumArgs() const;

  size_t NumReturns() const;

  bool ArgByRef(size_t arg_index) const;

  size_t ArgIdCount(size_t arg_index) const;

  ObjectID ArgId(size_t arg_index, size_t id_index) const;

  ObjectID ReturnId(size_t return_index, TaskTransportType transport_type) const;

  ObjectID ReturnIdForPlasma(size_t return_index) const {
    return ReturnId(return_index, TaskTransportType::RAYLET);
  }

  const uint8_t *ArgData(size_t arg_index) const;

  size_t ArgDataSize(size_t arg_index) const;

  const uint8_t *ArgMetadata(size_t arg_index) const;

  size_t ArgMetadataSize(size_t arg_index) const;

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

  bool IsDriverTask() const;

  Language GetLanguage() const;

  /// Whether this task is a normal task.
  bool IsNormalTask() const;

  /// Whether this task is an actor creation task.
  bool IsActorCreationTask() const;

  /// Whether this task is an actor task.
  bool IsActorTask() const;

  // Methods specific to actor creation tasks.

  ActorID ActorCreationId() const;

  uint64_t MaxActorReconstructions() const;

  std::vector<std::string> DynamicWorkerOptions() const;

  // Methods specific to actor tasks.

  ActorID ActorId() const;

  TaskID CallerId() const;

  uint64_t ActorCounter() const;

  ObjectID ActorCreationDummyObjectId() const;

  ObjectID PreviousActorTaskDummyObjectId() const;

  bool IsDirectCall() const;

  int MaxActorConcurrency() const;

  bool IsDetachedActor() const;

  ObjectID ActorDummyObject() const;

  std::string DebugString() const;

  static SchedulingClassDescriptor &GetSchedulingClassDescriptor(SchedulingClass id);

 private:
  void ComputeResources();

  /// Field storing required resources. Initalized in constructor.
  /// TODO(ekl) consider optimizing the representation of ResourceSet for fast copies
  /// instead of keeping shared ptrs here.
  std::shared_ptr<ResourceSet> required_resources_;
  /// Field storing required placement resources. Initalized in constructor.
  std::shared_ptr<ResourceSet> required_placement_resources_;
  /// Cached scheduling class of this task.
  SchedulingClass sched_cls_id_;

  /// Keep global static id mappings for SchedulingClass for performance.
  static std::unordered_map<SchedulingClassDescriptor, SchedulingClass> sched_cls_to_id_;
  static std::unordered_map<SchedulingClass, SchedulingClassDescriptor> sched_id_to_cls_;
  static int next_sched_id_;
};

}  // namespace ray

/// We must define the hash since it's not auto-defined for vectors.
namespace std {
template <>
struct hash<ray::SchedulingClassDescriptor> {
  size_t operator()(ray::SchedulingClassDescriptor const &k) const {
    size_t seed = std::hash<ray::ResourceSet>()(k.first);
    for (const auto &str : k.second) {
      seed ^= std::hash<std::string>()(str);
    }
    return seed;
  }
};
}  // namespace std

#endif  // RAY_COMMON_TASK_TASK_SPEC_H
