
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ActorType>
class ActorCreator {
 public:
  ActorCreator();

  ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
               std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  ActorHandle<ActorType> Remote();

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator() {}

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                      std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}

template <typename ActorType>
ActorHandle<ActorType> ActorCreator<ActorType>::Remote() {
  auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
  return ActorHandle<ActorType>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
