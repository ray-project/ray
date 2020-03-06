
#pragma once

#include <iostream>
#include <memory>

#include <msgpack.hpp>
#include "api/ray_api.h"
#include "api/task_type.h"

/**
 * ray api definition
 *
 */
namespace ray {

template <typename T>
class RayObject;
template <typename T>
class RayActor;
template <typename F>
class RayFunction;
template <typename T>
class WaitResult;

class Ray {
  template <typename T>
  friend class RayObject;

 private:
  static RayApi *_impl;

  template <typename T>
  static std::shared_ptr<T> get(const RayObject<T> &object);

 public:
  static void init();

  // static bool init(const RayConfig& rayConfig);

  template <typename T>
  static RayObject<T> put(const T &obj);

  template <typename T>
  static std::vector<std::shared_ptr<T>> get(const std::vector<RayObject<T>> &objects);

  template <typename T>
  static WaitResult<T> wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms);

#include "api/impl/call_funcs.generated.h"

#include "api/impl/create_actors.generated.h"

#include "api/impl/call_actors.generated.h"
};

}  // namespace ray

// --------- inline implementation ------------
#include "api/execute.h"
#include "api/impl/arguments.h"
#include "api/ray_actor.h"
#include "api/ray_function.h"
#include "api/ray_object.h"
#include <ray/api/wait_result.h>

namespace ray {
class Arguments;

template <typename T>
inline RayObject<T> Ray::put(const T &obj) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, obj);
  auto id = _impl->put(buffer);
  return RayObject<T>(id);
}

template <typename T>
inline std::shared_ptr<T> Ray::get(const RayObject<T> &object) {
  auto data = _impl->get(object.id());
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(data->size());
  memcpy(unpacker.buffer(), data->data(), data->size());
  unpacker.buffer_consumed(data->size());
  std::shared_ptr<T> rt(new T);
  Arguments::unwrap(unpacker, *rt);
  return rt;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::get(const std::vector<RayObject<T>> &objects) {
  return std::vector<std::shared_ptr<T>>();
}

template <typename T>
inline WaitResult<T> Ray::wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms) {
  return WaitResult<T>();
}

#include "api/impl/call_funcs_impl.generated.h"

#include "api/impl/create_actors_impl.generated.h"

#include "api/impl/call_actors_impl.generated.h"

}  // namespace ray
