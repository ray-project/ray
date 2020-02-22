#include "ray/core_worker/common.h"

namespace ray {

std::string WorkerTypeString(WorkerType type) {
  if (type == WorkerType::DRIVER) {
    return "driver";
  } else if (type == WorkerType::WORKER) {
    return "worker";
  }
  RAY_CHECK(false);
  return "";
}

std::string LanguageString(Language language) {
  if (language == Language::PYTHON) {
    return "python";
  } else if (language == Language::JAVA) {
    return "java";
  }
  RAY_CHECK(false);
  return "";
}

RayFunction::RayFunction() {}

RayFunction::RayFunction(Language language,
                         const ray::FunctionDescriptor &function_descriptor)
    : language_(language), function_descriptor_(function_descriptor) {}

const ray::FunctionDescriptor &RayFunction::GetFunctionDescriptor() const {
  return function_descriptor_;
}

TaskArg TaskArg::PassByReference(const ObjectID &object_id) {
  return TaskArg(std::make_shared<ObjectID>(object_id), nullptr);
}

const ObjectID &TaskArg::GetReference() const {
  RAY_CHECK(id_ != nullptr) << "This argument isn't passed by reference.";
  return *id_;
}

const RayObject &TaskArg::GetValue() const {
  RAY_CHECK(value_ != nullptr) << "This argument isn't passed by value.";
  return *value_;
}

TaskOptions::TaskOptions() {}

ActorCreationOptions::ActorCreationOptions() {}

}  // namespace ray
