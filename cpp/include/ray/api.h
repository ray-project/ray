
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

class Ray {
  template <typename T>
  friend class RayObject;

 private:
  static RayApi *_impl;

  template <typename T>
  static bool get(const UniqueId &id, T &obj);

 public:
  static void init();

  // static bool init(const RayConfig& rayConfig);

  template <typename T>
  static std::unique_ptr<RayObject<T>> put(const T &obj);

  static uint64_t wait(const UniqueId *pids, int count, int minNumReturns,
                       int timeoutMilliseconds);

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

namespace ray {
class Arguments;

template <typename T>
inline std::unique_ptr<RayObject<T>> Ray::put(const T &obj) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, obj);
  auto id = _impl->put(buffer);
  std::unique_ptr<RayObject<T>> ptr(new RayObject<T>(*id));
  return ptr;
}

template <typename T>
inline bool Ray::get(const UniqueId &id, T &obj) {
  auto data = _impl->get(id);
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(data->size());
  memcpy(unpacker.buffer(), data->data(), data->size());
  unpacker.buffer_consumed(data->size());
  Arguments::unwrap(unpacker, obj);
  return true;
}

#include "api/impl/call_funcs_impl.generated.h"

#include "api/impl/create_actors_impl.generated.h"

#include "api/impl/call_actors_impl.generated.h"

}  // namespace ray
