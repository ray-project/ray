
#pragma once

#include <ray/api/actor_handle.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename ActorType, typename F = void>
class ActorCreator {
 public:
  ActorCreator();

  ActorCreator(RayRuntime *runtime, std::string create_func_name)
      : runtime_(runtime), create_func_name_(std::move(create_func_name)) {}

  ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
               std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  template <typename... Args>
  ActorHandle<ActorType> Remote(Args... args) {
    if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
      StaticCheck<F, Args...>();
      Arguments::WrapArgs(&args_, create_func_name_, args...);
    }

    auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
    return ActorHandle<ActorType>(returned_actor_id);
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_{};
  std::string create_func_name_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ActorType, typename F>
ActorCreator<ActorType, F>::ActorCreator() {}

template <typename ActorType, typename F>
ActorCreator<ActorType, F>::ActorCreator(
    RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
    std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}
}  // namespace api
}  // namespace ray
