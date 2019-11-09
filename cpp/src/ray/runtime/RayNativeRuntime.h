
#pragma once

#include "RayRuntime.h"

namespace ray {

class RayNativeRuntime : public RayRuntime {
  friend class RayRuntime;

 private:
  RayNativeRuntime(std::shared_ptr<RayConfig> params);
};

}  // namespace ray