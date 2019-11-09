
#pragma once

#include "RayObject.h"

namespace ray {

template <typename T>
class RayActor : public RayObject<T> {
 public:
  RayActor(UniqueId id) : RayObject<T>(id) {}
};
}
