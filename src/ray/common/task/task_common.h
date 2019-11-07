#ifndef RAY_COMMON_TASK_TASK_COMMON_H
#define RAY_COMMON_TASK_TASK_COMMON_H

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/protobuf/common.pb.h"

namespace ray {

// NOTE(hchen): Below we alias `ray::rpc::Language|TaskType)` in  `ray` namespace.
// The reason is because other code should use them as if they were defined in this
// `task_common.h` file, shouldn't care about the implementation detail that they
// are defined in protobuf.

/// See `common.proto` for definition of `Language` enum.
using Language = rpc::Language;
/// See `common.proto` for definition of `TaskType` enum.
using TaskType = rpc::TaskType;

/// Information about a remote function.
class RayFunction {
 public:
  RayFunction() {}
  RayFunction(Language language, const std::vector<std::string> &function_descriptor)
      : language_(language), function_descriptor_(function_descriptor) {}

  Language GetLanguage() const { return language_; }

  const std::vector<std::string> &GetFunctionDescriptor() const {
    return function_descriptor_;
  }

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

}  // namespace ray

#endif
