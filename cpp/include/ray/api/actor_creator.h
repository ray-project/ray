
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ActorType>
class ActorCreator {
 public:
  ActorCreator();

  ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
               std::shared_ptr<msgpack::sbuffer> args);

  ActorHandle<ActorType> Remote();

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::shared_ptr<msgpack::sbuffer> args_;
};

// ---------- implementation ----------

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator() {}

template <typename ActorType>
ActorCreator<ActorType>::ActorCreator(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                      std::shared_ptr<msgpack::sbuffer> args)
    : runtime_(runtime), ptr_(ptr), args_(args) {}

template <typename ActorType>
ActorHandle<ActorType> ActorCreator<ActorType>::Remote() {
  auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
  return ActorHandle<ActorType>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
