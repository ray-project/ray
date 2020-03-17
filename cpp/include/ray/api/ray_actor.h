
#pragma once

#include <ray/core.h>

namespace ray {
namespace api {

#include <ray/api/generated/actor_funcs.generated.h>

template <typename O>
class RayActor {
 public:
  RayActor();

  RayActor(const ActorID &id);

  RayActor(const ActorID &&id);

  const ActorID &ID() const;

#include <ray/api/generated/actor_call.generated.h>

  MSGPACK_DEFINE(id_);

 private:
  ActorID id_;
};

template <typename O>
RayActor<O>::RayActor() {}

template <typename O>
RayActor<O>::RayActor(const ActorID &id) {
  id_ = id;
}

template <typename O>
RayActor<O>::RayActor(const ActorID &&id) {
  id_ = std::move(id);
}

template <typename O>
const ActorID &RayActor<O>::ID() const {
  return id_;
}

#include <ray/api/generated/actor_call_impl.generated.h>
}  // namespace api
}  // namespace ray
