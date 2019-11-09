
#pragma once

#include <ray/api/UniqueId.h>
#include <ray/core.h>
#include <unordered_map>
#include "RayRuntime.h"

namespace ray {

class RayDevRuntime : public RayRuntime {
  friend class RayRuntime;

 private:
  static std::unordered_map<UniqueId, char *> _actors;

  RayDevRuntime(std::shared_ptr<RayConfig> params);

  static std::unique_ptr<UniqueId> createActor(remote_function_ptr_holder &fptr,
                                               std::vector< ::ray::blob> &&args);

  char *get_actor_ptr(const UniqueId &id);

  std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                   std::vector< ::ray::blob> &&args);
};

}  // namespace ray