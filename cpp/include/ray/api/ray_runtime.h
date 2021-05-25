
#pragma once

#include <ray/api/function_manager.h>

#include <cstdint>
#include <memory>
#include <msgpack.hpp>
#include <typeinfo>
#include <vector>

#include "ray/core.h"

namespace ray {
namespace api {

struct RemoteFunctionHolder {
  RemoteFunctionHolder() = default;
  template <typename F>
  RemoteFunctionHolder(F func) {
    auto func_name = ray::internal::FunctionManager::Instance().GetFunctionName(func);
    if (func_name.empty()) {
      throw RayException(
          "Function not found. Please use RAY_REMOTE to register this function.");
    }
    function_name = std::move(func_name);
  }

  /// The remote function name.
  std::string function_name;
};

class RayRuntime {
 public:
  virtual ObjectID Put(std::shared_ptr<msgpack::sbuffer> data) = 0;
  virtual std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &ids) = 0;

  virtual std::vector<bool> Wait(const std::vector<ObjectID> &ids, int num_objects,
                                 int timeout_ms) = 0;

  virtual ObjectID Call(const RemoteFunctionHolder &remote_function_holder,
                        std::vector<ray::api::TaskArg> &args) = 0;
  virtual ActorID CreateActor(const RemoteFunctionHolder &remote_function_holder,
                              std::vector<ray::api::TaskArg> &args) = 0;
  virtual ObjectID CallActor(const RemoteFunctionHolder &remote_function_holder,
                             const ActorID &actor,
                             std::vector<ray::api::TaskArg> &args) = 0;
};

void AddLocalReference(const ObjectID &id);

void RemoveLocalReference(const ObjectID &id);

}  // namespace api
}  // namespace ray