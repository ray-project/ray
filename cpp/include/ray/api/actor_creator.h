
#pragma once

#include <ray/api/actor_handle.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename ActorType>
class ActorCreator {
 public:
  ActorCreator();

  ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr)
      : runtime_(runtime), ptr_(ptr) {}

  template <typename... Args>
  ActorHandle<ActorType> Remote(Args... args);

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator() {}

template <typename ActorType>
template <typename... Args>
ActorHandle<ActorType> ActorCreator<ActorType>::Remote(Args... args) {
  Arguments::WrapArgs(&args_, args...);
  auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
  return ActorHandle<ActorType>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
