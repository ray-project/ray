
#pragma once

#include <ray/api/ray_object.h>

namespace ray {

template <typename O>
class RayActor : public RayObject<O> {
 public:
  RayActor(UniqueId id) : RayObject<O>(id) {}
  MSGPACK_DEFINE(MSGPACK_BASE(RayObject<O>));

#include <ray/api/impl/actor_call.generated.h>
};

#include <ray/api/impl/actor_call_impl.generated.h>
}  // namespace ray
