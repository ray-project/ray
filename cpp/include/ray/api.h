
#pragma once

#include <memory>

#include "api/RayApi.h"
#include "api/Blob.h"
#include "api/impl/FunctionArgument.h"
#include "ray/util/type-util.h"

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

#include "api/impl/CallFuncs.generated.h"

#include "api/impl/CreateActors.generated.h"

#include "api/impl/CallActors.generated.h"
};

}  // namespace ray

// --------- inline implementation ------------
#include "api/Execute.h"
#include "api/RayActor.h"
#include "api/RayFunction.h"
#include "api/RayObject.h"
#include "api/impl/Arguments.h"

namespace ray {
class Arguments;

template <typename T>
inline std::unique_ptr<RayObject<T> > Ray::put(const T &obj) {
  ::ray::binary_writer writer;
  Arguments::wrap(writer, obj);
  std::vector< ::ray::blob> data;
  writer.get_buffers(data);
  auto id = _impl->put(std::move(data));
  std::unique_ptr<RayObject<T> > ptr(new RayObject<T>(*id));
  return ptr;
}

template <typename T>
inline bool Ray::get(const UniqueId &id, T &obj) {
  auto data = _impl->get(id);
  ::ray::binary_reader reader(*data.get());
  Arguments::unwrap(reader, obj);
  return true;
}

#include "api/impl/CallFuncsImpl.generated.h"

#include "api/impl/CreateActorsImpl.generated.h"

#include "api/impl/CallActorsImpl.generated.h"

}  // namespace ray
