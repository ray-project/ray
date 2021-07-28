
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace runtime {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime();
};

}  // namespace runtime
}  // namespace ray