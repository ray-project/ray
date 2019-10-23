#ifndef RAY_CORE_WORKER_COMMON_H
#define RAY_CORE_WORKER_COMMON_H

#include <string>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"

namespace ray {
using WorkerType = rpc::WorkerType;

// Return a string representation of the worker type.
std::string WorkerTypeString(WorkerType type);

// Return a string representation of the language.
std::string LanguageString(Language language);

/// Information about a remote function.
class RayFunction {
 public:
  RayFunction() {}
  RayFunction(Language language, const std::vector<std::string> &function_descriptor)
      : language_(language), function_descriptor_(function_descriptor) {}

  Language GetLanguage() const { return language_; }

  std::vector<std::string> GetFunctionDescriptor() const { return function_descriptor_; }

 private:
  Language language_;
  std::vector<std::string> function_descriptor_;
};

/// Argument of a task.
class TaskArg {
 public:
  /// Create a pass-by-reference task argument.
  ///
  /// \param[in] object_id Id of the argument.
  /// \return The task argument.
  static TaskArg PassByReference(const ObjectID &object_id) {
    return TaskArg(std::make_shared<ObjectID>(object_id), nullptr);
  }

  /// Create a pass-by-value task argument.
  ///
  /// \param[in] value Value of the argument.
  /// \return The task argument.
  static TaskArg PassByValue(const std::shared_ptr<RayObject> &value) {
    RAY_CHECK(value) << "Value can't be null.";
    return TaskArg(nullptr, value);
  }

  /// Return true if this argument is passed by reference, false if passed by value.
  bool IsPassedByReference() const { return id_ != nullptr; }

  /// Get the reference object ID.
  const ObjectID &GetReference() const {
    RAY_CHECK(id_ != nullptr) << "This argument isn't passed by reference.";
    return *id_;
  }

  /// Get the value.
  const RayObject &GetValue() const {
    RAY_CHECK(value_ != nullptr) << "This argument isn't passed by value.";
    return *value_;
  }

 private:
  TaskArg(const std::shared_ptr<ObjectID> id, const std::shared_ptr<RayObject> value)
      : id_(id), value_(value) {}

  /// Id of the argument if passed by reference, otherwise nullptr.
  const std::shared_ptr<ObjectID> id_;
  /// Value of the argument if passed by value, otherwise nullptr.
  const std::shared_ptr<RayObject> value_;
};

enum class StoreProviderType { PLASMA, MEMORY };

enum class TaskTransportType { RAYLET, DIRECT_ACTOR };

/// Options for all tasks (actor and non-actor) except for actor creation.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(int num_returns, std::unordered_map<std::string, double> &resources)
      : num_returns(num_returns), resources(resources) {}

  /// Number of returns of this task.
  int num_returns = 1;
  /// Resources required by this task.
  std::unordered_map<std::string, double> resources;
};

/// Options for actor creation tasks.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(uint64_t max_reconstructions, bool is_direct_call,
                       const std::unordered_map<std::string, double> &resources,
                       const std::unordered_map<std::string, double> &placement_resources,
                       const std::vector<std::string> &dynamic_worker_options)
      : max_reconstructions(max_reconstructions),
        is_direct_call(is_direct_call),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options) {}

  /// Maximum number of times that the actor should be reconstructed when it dies
  /// unexpectedly. It must be non-negative. If it's 0, the actor won't be reconstructed.
  const uint64_t max_reconstructions = 0;
  /// Whether to use direct actor call. If this is set to true, callers will submit
  /// tasks directly to the created actor without going through raylet.
  const bool is_direct_call = false;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
  /// Resources required to place this actor.
  const std::unordered_map<std::string, double> placement_resources;
  /// The dynamic options used in the worker command when starting a worker process for
  /// an actor creation task.
  const std::vector<std::string> dynamic_worker_options;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
