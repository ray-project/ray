
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

  MSGPACK_DEFINE(_id);

 private:
  ActorID _id;
};

template <typename O>
RayActor<O>::RayActor() {}

template <typename O>
RayActor<O>::RayActor(const ActorID &id) {
  _id = id;
}

template <typename O>
RayActor<O>::RayActor(const ActorID &&id) {
  _id = std::move(id);
}

template <typename O>
const ActorID &RayActor<O>::ID() const {
  return _id;
}

#include <ray/api/generated/actor_call_impl.generated.h>
}  // namespace api
}  // namespace ray
