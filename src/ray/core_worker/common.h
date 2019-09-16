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

/// Information about a remote function.
struct RayFunction {
  /// Language of the remote function.
  const Language language;
  /// Function descriptor of the remote function.
  const std::vector<std::string> function_descriptor;
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

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
