
#pragma once

#include <ray/api/ray_object.h>

namespace ray {

#include <ray/api/generated/actor_funcs.generated.h>

template <typename O>
class RayActor : public RayObject<O> {
 public:
  RayActor(UniqueId id) : RayObject<O>(id) {};
  MSGPACK_DEFINE(MSGPACK_BASE(RayObject<O>));

#include <ray/api/generated/actor_call.generated.h>
};

#include <ray/api/generated/actor_call_impl.generated.h>
}  // namespace ray
