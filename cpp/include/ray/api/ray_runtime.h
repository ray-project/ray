
#pragma once

#include <cstdint>
#include <memory>
#include <msgpack.hpp>
#include <typeinfo>
#include <vector>

#include <ray/api/wait_result.h>
#include <ray/core.h>

namespace ray {
namespace api {

struct member_function_ptr_holder {
  uintptr_t value[2];
};

struct remote_function_ptr_holder {
  uintptr_t value[2];
};

class RayRuntime {
 public:
  virtual ObjectID Put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &objects) = 0;

  virtual WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                          int64_t timeout_ms) = 0;

  virtual ObjectID Call(remote_function_ptr_holder &fptr,
                        std::shared_ptr<msgpack::sbuffer> args) = 0;
  virtual ActorID Create(remote_function_ptr_holder &fptr,
                         std::shared_ptr<msgpack::sbuffer> args) = 0;
  virtual ObjectID Call(const remote_function_ptr_holder &fptr, const ActorID &actor,
                        std::shared_ptr<msgpack::sbuffer> args) = 0;
};
}  // namespace api
}  // namespace ray