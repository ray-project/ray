
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class ActorTaskCaller {
 public:
  ActorTaskCaller();

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
                  std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  ObjectRef<ReturnType> Remote();

 private:
  RayRuntime *runtime_;
  ActorID id_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
ActorTaskCaller<ReturnType>::ActorTaskCaller() {}

template <typename ReturnType>
ActorTaskCaller<ReturnType>::ActorTaskCaller(
    RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
    std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), id_(id), ptr_(ptr), args_(std::move(args)) {}

template <typename ReturnType>
ObjectRef<ReturnType> ActorTaskCaller<ReturnType>::Remote() {
  auto returned_object_id = runtime_->CallActor(ptr_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace api
}  // namespace ray
