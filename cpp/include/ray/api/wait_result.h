
#pragma once

#include <vector>
#include <ray/core.h>

namespace ray { namespace api {

class WaitResult {
 public:
  std::vector<ObjectID> readys;
  std::vector<ObjectID> remains;
  WaitResult(){};
  WaitResult(std::vector<ObjectID> &&objectReadys,
                     std::vector<ObjectID> &&objectRemains)
      : readys(std::move(objectReadys)), remains(std::move(objectRemains)){};
};
}  }// namespace ray::api