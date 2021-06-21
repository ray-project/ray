
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace api {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime();
};

}  // namespace api
}  // namespace ray