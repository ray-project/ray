
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
             std::shared_ptr<msgpack::sbuffer> args);

  ObjectRef<ReturnType> Remote();

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::shared_ptr<msgpack::sbuffer> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller() {}

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                   std::shared_ptr<msgpack::sbuffer> args)
    : runtime_(runtime), ptr_(ptr), args_(args) {}

template <typename ReturnType>
ObjectRef<ReturnType> TaskCaller<ReturnType>::Remote() {
  auto returned_object_id = runtime_->Call(ptr_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace api
}  // namespace ray
