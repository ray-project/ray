
#pragma once

#include <vector>

namespace ray {
template <typename T>
class RayObject;

class UniqueId;

class WaitResultInternal {
 public:
  std::vector<UniqueId> readys;
  std::vector<UniqueId> remains;
  WaitResultInternal(){};
  WaitResultInternal(std::vector<UniqueId> &&objectReadys,
                     std::vector<UniqueId> &&objectRemains)
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
}  // namespace ray