
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr);

  template <typename... Args>
  ObjectRef<ReturnType> Remote(Args... args) {
    Arguments::WrapArgs(&args_, args...);
    ptr_.exec_function_pointer = reinterpret_cast<uintptr_t>(
        NormalExecFunction<ReturnType, typename FilterArgType<Args>::type...>);
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
TaskCaller<ReturnType>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr)
    : runtime_(runtime), ptr_(ptr) {}
}  // namespace api
}  // namespace ray
