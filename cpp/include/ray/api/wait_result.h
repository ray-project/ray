
#pragma once

#include <ray/core.h>
#include <vector>

namespace ray {
namespace api {

class WaitResult {
 public:
  std::vector<ObjectID> readys;
  std::vector<ObjectID> remains;
  WaitResult(){};
  WaitResult(std::vector<ObjectID> &&objectReadys, std::vector<ObjectID> &&objectRemains)
      : readys(std::move(objectReadys)), remains(std::move(objectRemains)){};
};
}  // namespace api
}  // namespace ray