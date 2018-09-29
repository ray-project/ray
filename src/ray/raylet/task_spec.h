#ifndef RAY_RAYLET_TASK_SPECIFICATION_H
#define RAY_RAYLET_TASK_SPECIFICATION_H

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/function_descriptor.h"
#include "format/common_generated.h"
#include "ray/id.h"
#include "ray/raylet/scheduling_resources.h"

extern "C" {
#include "sha256.h"
}

namespace ray {

namespace raylet {

/// \class TaskArgument
///
/// A virtual class that represents an argument to a task.
class TaskArgument {
 public:
  /// Serialize the task argument to a flatbuffer.
  ///
  /// \param fbb The flatbuffer builder to serialize with.
  /// \return An offset to the serialized task argument.
  virtual flatbuffers::Offset<Arg> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const = 0;

  virtual ~TaskArgument() = 0;
};

/// \class TaskArgumentByReference
///
/// A task argument consisting of a list of object ID references.
class TaskArgumentByReference : virtual public TaskArgument {
 public:
  /// Create a task argument by reference from a list of object IDs.
  ///
  /// \param references A list of object ID references.
  TaskArgumentByReference(const std::vector<ObjectID> &references);

  ~TaskArgumentByReference(){};

  flatbuffers::Offset<Arg> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const;

 private:
  /// The object IDs.
  const std::vector<ObjectID> references_;
};

/// \class TaskArgumentByValue
///
/// A task argument containing the raw value.
class TaskArgumentByValue : public TaskArgument {
 public:
  /// Create a task argument from a raw value.
  ///
  /// \param value A pointer to the raw value.
  /// \param length The size of the raw value.
  TaskArgumentByValue(const uint8_t *value, size_t length);

  flatbuffers::Offset<Arg> ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const;

 private:
  /// The raw value.
  std::vector<uint8_t> value_;
};

/// \class TaskSpecification
///
/// The task specification encapsulates all immutable information about the
/// task. These fields are determined at submission time, converse to the
/// TaskExecutionSpecification that may change at execution time.
class TaskSpecification {
 public:
  /// Deserialize a task specification from a flatbuffer.
  ///
  /// \param string A serialized task specification flatbuffer.
  TaskSpecification(const flatbuffers::String &string);

  // TODO(swang): Define an actor task constructor.
  /// Create a task specification from the raw fields. This constructor omits
  /// some values and sets them to sensible defaults.
  ///
  /// \param driver_id The driver ID, representing the job that this task is a
  /// part of.
  /// \param parent_task_id The task ID of the task that spawned this task.
  /// \param parent_counter The number of tasks that this task's parent spawned
  /// before this task.
  /// \param function_descriptor The function descriptor.
  /// \param task_arguments The list of task arguments.
  /// \param num_returns The number of values returned by the task.
  /// \param required_resources The task's resource demands.
  /// \param language The language of the worker that must execute the function.
  TaskSpecification(const UniqueID &driver_id, const TaskID &parent_task_id,
                    int64_t parent_counter,
                    const std::vector<std::shared_ptr<TaskArgument>> &task_arguments,
                    int64_t num_returns,
                    const std::unordered_map<std::string, double> &required_resources,
                    const Language &language,
                    const FunctionDescriptor &function_descriptor);

  // TODO(swang): Define an actor task constructor.
  /// Create a task specification from the raw fields.
  ///
  /// \param driver_id The driver ID, representing the job that this task is a
  /// part of.
  /// \param parent_task_id The task ID of the task that spawned this task.
  /// \param parent_counter The number of tasks that this task's parent spawned
  /// before this task.
  /// \param actor_creation_id If this is an actor task, then this is the ID of
  /// the corresponding actor creation task. Otherwise, this is nil.
  /// \param actor_id The ID of the actor for the task. If this is not an actor
  /// task, then this is nil.
  /// \param actor_handle_id The ID of the actor handle that submitted this
  /// task. If this is not an actor task, then this is nil.
  /// \param actor_counter The number of tasks submitted before this task from
  /// the same actor handle. If this is not an actor task, then this is 0.
  /// \param function_id The ID of the function this task should execute.
  /// \param task_arguments The list of task arguments.
  /// \param num_returns The number of values returned by the task.
  /// \param required_resources The task's resource demands.
  /// \param required_placement_resources The resources required to place this
  /// task on a node. Typically, this should be an empty map in which case it
  /// will default to be equal to the required_resources argument.
  /// \param language The language of the worker that must execute the function.
  TaskSpecification(
      const UniqueID &driver_id, const TaskID &parent_task_id, int64_t parent_counter,
      const ActorID &actor_creation_id, const ObjectID &actor_creation_dummy_object_id,
      const ActorID &actor_id, const ActorHandleID &actor_handle_id,
      int64_t actor_counter, const FunctionID &function_id,
      const std::vector<std::shared_ptr<TaskArgument>> &task_arguments,
      int64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources,
      const Language &language,
      const FunctionDescriptor &function_descriptor);

  /// Deserialize a task specification from a flatbuffer's string data.
  ///
  /// \param string The string data for a serialized task specification
  /// flatbuffer.
  TaskSpecification(const std::string &string);

  ~TaskSpecification() {}

  /// Serialize the TaskSpecification to a flatbuffer.
  ///
  /// \param fbb The flatbuffer builder to serialize with.
  /// \return An offset to the serialized task specification.
  flatbuffers::Offset<flatbuffers::String> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

  // TODO(swang): Finalize and document these methods.
  TaskID TaskId() const;
  UniqueID DriverId() const;
  TaskID ParentTaskId() const;
  int64_t ParentCounter() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  FunctionID FunctionId() const;
  int64_t NumArgs() const;
  int64_t NumReturns() const;
  bool ArgByRef(int64_t arg_index) const;
  int ArgIdCount(int64_t arg_index) const;
  ObjectID ArgId(int64_t arg_index, int64_t id_index) const;
  ObjectID ReturnId(int64_t return_index) const;
  const uint8_t *ArgVal(int64_t arg_index) const;
  size_t ArgValLength(int64_t arg_index) const;
  double GetRequiredResource(const std::string &resource_name) const;
  /// Return the resources that are to be acquired during the execution of this
  /// task.
  ///
  /// \return The resources that will be acquired during the execution of this
  /// task.
  const ResourceSet GetRequiredResources() const;
  /// Return the resources that are required for a task to be placed on a node.
  /// This will typically be the same as the resources acquired during execution
  /// and will always be a superset of those resources. However, they may
  /// differ, e.g., actor creation tasks may require more resources to be
  /// scheduled on a machine because the actor creation task may require no
  /// resources itself, but subsequent actor methods may require resources, and
  /// so the placement of the actor should take this into account.
  ///
  /// \return The resources that are required to place a task on a node.
  const ResourceSet GetRequiredPlacementResources() const;
  bool IsDriverTask() const;
  Language GetLanguage() const;

  // Methods specific to actor tasks.
  bool IsActorCreationTask() const;
  bool IsActorTask() const;
  ActorID ActorCreationId() const;
  ObjectID ActorCreationDummyObjectId() const;
  ActorID ActorId() const;
  ActorHandleID ActorHandleId() const;
  int64_t ActorCounter() const;
  ObjectID ActorDummyObject() const;

 private:
  /// Assign the specification data from a pointer.
  void AssignSpecification(const uint8_t *spec, size_t spec_size);
  /// Get a pointer to the byte data.
  const uint8_t *data() const;
  /// Get the size in bytes of the task specification.
  size_t size() const;

  /// The task specification data.
  std::vector<uint8_t> spec_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_SPECIFICATION_H
