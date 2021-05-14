
#pragma once

#include <ray/api/wait_result.h>

#include <cstdint>
#include <memory>
#include <msgpack.hpp>
#include <typeinfo>
#include <vector>

#include "ray/core.h"

namespace ray {
namespace api {

struct RemoteFunctionHolder {
  /// The remote function name.
  std::string function_name;
};

class RayRuntime {
 public:
  virtual ObjectID Put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &ids) = 0;

  virtual WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                          int timeout_ms) = 0;

  virtual ObjectID Call(const RemoteFunctionHolder &fptr,
                        std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ActorID CreateActor(const RemoteFunctionHolder &fptr,
                              std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ObjectID CallActor(const RemoteFunctionHolder &fptr, const ActorID &actor,
                             std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
};

}  // namespace api
}  // namespace ray