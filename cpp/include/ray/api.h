
#pragma once

#include <memory>
#include <iostream> 

#include "api/blob.h"
#include "api/impl/function_argument.h"
#include "api/ray_api.h"
#include "ray/util/type_util.h"

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
  ::ray::binary_writer writer;
  Arguments::wrap(writer, obj);
  std::vector<::ray::blob> data;
  writer.get_buffers(data);
  auto id = _impl->put(std::move(data));
  std::unique_ptr<RayObject<T>> ptr(new RayObject<T>(*id));
  return ptr;
}

template <typename T>
inline bool Ray::get(const UniqueId &id, T &obj) {
  auto data = _impl->get(id);
  ::ray::binary_reader reader(*data.get());
  Arguments::unwrap(reader, obj);
  return true;
}

#include "api/impl/call_funcs_impl.generated.h"

#include "api/impl/create_actors_impl.generated.h"

#include "api/impl/call_actors_impl.generated.h"

}  // namespace ray
