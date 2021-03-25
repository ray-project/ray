
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
             std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  template <typename... Args>
  ObjectRef<ReturnType> Remote(Args... args) {
    auto returned_object_id = runtime_->Call(ptr_, args_);
    return ObjectRef<ReturnType>(returned_object_id);
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_{};
  std::string function_name_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller() {}

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                   std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}
}  // namespace api
}  // namespace ray
