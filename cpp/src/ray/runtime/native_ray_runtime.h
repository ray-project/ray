
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace internal {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime();
};

}  // namespace internal
}  // namespace ray