
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

struct MemberFunctionPtrHolder {
  uintptr_t value[2];
};

struct RemoteFunctionPtrHolder {
  /// The remote function pointer
  uintptr_t function_pointer;
  /// The executable function pointer
  uintptr_t exec_function_pointer;
};

class RayRuntime {
 public:
  virtual ObjectID Put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &ids) = 0;

  virtual WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                          int timeout_ms) = 0;

  virtual ObjectID Call(const RemoteFunctionPtrHolder &fptr,
                        std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ActorID CreateActor(const RemoteFunctionPtrHolder &fptr,
                              std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
  virtual ObjectID CallActor(const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
                             std::vector<std::unique_ptr<::ray::TaskArg>> &args) = 0;
};
}  // namespace api
}  // namespace ray