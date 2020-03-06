
#pragma once

#include <vector>

// #include "uniqueId.h"
// #include "ray_object.h"

namespace ray {
  template <typename T>
  class RayObject;

  class UniqueId;

  template <typename T>
  class WaitResult {
    public:
    std::vector<RayObject<T>> readys;
    std::vector<RayObject<T>> remains;
  };

  class WaitResultInternal {
    public:
    std::vector<UniqueId> readys;
    std::vector<UniqueId> remains;
  };

}  // namespace ray