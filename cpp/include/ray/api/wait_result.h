
#pragma once

#include <list>

#include <ray/api/object_ref.h>
#include "ray/core.h"

namespace ray {
namespace api {

/// \param T The type of object.
template <typename T>
class WaitResult {
 public:
  /// The object id list of ready objects
  std::list<ObjectRef<T>> ready;
  /// The object id list of unready objects
  std::list<ObjectRef<T>> unready;
  WaitResult(){};
  WaitResult(std::list<ObjectRef<T>> &&ready_objects,
             std::list<ObjectRef<T>> &&unready_objects)
      : ready(ready_objects), unready(unready_objects){};
};
}  // namespace api
}  // namespace ray