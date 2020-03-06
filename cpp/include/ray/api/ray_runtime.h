
#pragma once

#include <cstdint>
#include <memory>
#include <msgpack.hpp>
#include <typeinfo>
#include <vector>

#include <ray/api/uniqueId.h>
#include <ray/api/wait_result.h>

namespace ray {

struct member_function_ptr_holder {
  uintptr_t value[2];
};

struct remote_function_ptr_holder {
  uintptr_t value[2];
};

class RayRuntime {
 public:
  virtual UniqueId put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> get(const UniqueId &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> get(
      const std::vector<UniqueId> &objects) = 0;

  virtual WaitResultInternal wait(const std::vector<UniqueId> &objects, int num_objects,
                                  int64_t timeout_ms) = 0;

  virtual std::unique_ptr<UniqueId> call(remote_function_ptr_holder &fptr,
                                         std::shared_ptr<msgpack::sbuffer> args) = 0;
  virtual std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                           std::shared_ptr<msgpack::sbuffer> args) = 0;
  virtual std::unique_ptr<UniqueId> call(const remote_function_ptr_holder &fptr,
                                         const UniqueId &actor,
                                         std::shared_ptr<msgpack::sbuffer> args) = 0;
};
}  // namespace ray