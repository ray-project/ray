
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/util.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType, typename F = void>
class ActorTaskCaller {
 public:
  ActorTaskCaller();

  ActorTaskCaller(RayRuntime *runtime, ActorID id, std::string func_name)
      : runtime_(runtime), id_(id), func_name_(std::move(func_name)) {}

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
                  std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  template <typename... Args>
  ObjectRef<ReturnType> Remote(Args... args) {
    if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
      StaticCheck<F, Args...>();
      Arguments::WrapArgs(&args_, func_name_, args...);
    }
    auto returned_object_id = runtime_->CallActor(ptr_, id_, args_);
    return ObjectRef<ReturnType>(returned_object_id);
  }

 private:
  RayRuntime *runtime_;
  ActorID id_;
  std::string func_name_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType, typename F>
ActorTaskCaller<ReturnType, F>::ActorTaskCaller() {}

template <typename ReturnType, typename F>
ActorTaskCaller<ReturnType, F>::ActorTaskCaller(
    RayRuntime *runtime, ActorID id, RemoteFunctionPtrHolder ptr,
    std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), id_(id), ptr_(ptr), args_(std::move(args)) {}
}  // namespace api
}  // namespace ray
