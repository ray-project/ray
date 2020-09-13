
#pragma once

#include <vector>

#include "ray/core.h"

namespace ray {
namespace api {

class WaitResult {
 public:
  /// The object id array of ready objects
  std::vector<ObjectID> ready;
  /// The object id array of unready objects
  std::vector<ObjectID> unready;
  WaitResult(){};
  WaitResult(std::vector<ObjectID> &&ready_objects,
             std::vector<ObjectID> &&unready_objects)
      : ready(std::move(ready_objects)), unready(std::move(unready_objects)){};
};
}  // namespace api
}  // namespace ray