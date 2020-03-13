
#pragma once

#include <vector>
#include <ray/core.h>

namespace ray { namespace api {
template <typename T>
class RayObject;

class WaitResultInternal {
 public:
  std::vector<ObjectID> readys;
  std::vector<ObjectID> remains;
  WaitResultInternal(){};
  WaitResultInternal(std::vector<ObjectID> &&objectReadys,
                     std::vector<ObjectID> &&objectRemains)
      : readys(std::move(objectReadys)), remains(std::move(objectRemains)){};
};

template <typename T>
class WaitResult {
 public:
  std::vector<RayObject<T>> readys;
  std::vector<RayObject<T>> remains;
  WaitResult(){};
  WaitResult(std::vector<RayObject<T>> &&objectReadys,
             std::vector<RayObject<T>> &&objectRemains)
      : readys(std::move(objectReadys)), remains(std::move(objectRemains)){};
};
}  }// namespace ray::api