
#pragma once

#include <memory>

#include <ray/api/ray_runtime.h>
#include <ray/core.h>
#include <ray/api/generated/funcs.generated.h>
#include <ray/api/generated/create_funcs.generated.h>
#include <ray/api/generated/actor_funcs.generated.h>
#include <msgpack.hpp>
#include <ray/core.h>

/**
 * ray api definition
 *
 */
namespace ray { namespace api {

template <typename T>
class RayObject;
template <typename T>
class RayActor;
template <typename T>
class WaitResult;

class Ray {
  template <typename T>
  friend class RayObject;

 private:
  static RayRuntime *_impl;

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
  static WaitResult<T> wait(const std::vector<RayObject<T>> &objects, int num_objects,
                            int64_t timeout_ms);

#include "api/generated/call_funcs.generated.h"

#include "api/generated/create_actors.generated.h"

#include "api/generated/call_actors.generated.h"
};

}  }// namespace ray::api

// --------- inline implementation ------------
#include <ray/api/execute.h>
#include <ray/api/arguments.h>
#include <ray/api/ray_actor.h>
#include <ray/api/ray_object.h>
#include <ray/api/wait_result.h>

namespace ray { namespace api {
class Arguments;

template <typename T>
inline static std::vector<ObjectID> rayObject2ObjectID(
    const std::vector<RayObject<T>> &rayObjects) {
  std::vector<ObjectID> unqueIds;
  for (auto it = rayObjects.begin(); it != rayObjects.end(); it++) {
    unqueIds.push_back(it->id());
  }
  return unqueIds;
}

template <typename T>
inline static std::vector<RayObject<T>> ObjectID2RayObject(
    const std::vector<ObjectID> &objectIDs) {
  std::vector<RayObject<T>> objects;
  for (auto it = objectIDs.begin(); it != objectIDs.end(); it++) {
    objects.push_back(RayObject<T>(*it));
  }
  return objects;
}

template <typename T>
static WaitResult<T> waitResultFromInernal(const WaitResultInternal &internal) {
  return WaitResult<T>(std::move(ObjectID2RayObject<T>(internal.readys)),
                       std::move(ObjectID2RayObject<T>(internal.remains)));
}

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
inline std::vector<std::shared_ptr<T>> Ray::get(
    const std::vector<RayObject<T>> &objects) {
  auto uniqueVector = rayObject2ObjectID<T>(objects);
  auto result = _impl->get(uniqueVector);
  std::vector<std::shared_ptr<T>> rt;
  for (auto it = result.begin(); it != result.end(); it++) {
    msgpack::unpacker unpacker;
    unpacker.reserve_buffer((*it)->size());
    memcpy(unpacker.buffer(), (*it)->data(), (*it)->size());
    unpacker.buffer_consumed((*it)->size());
    std::shared_ptr<T> obj(new T);
    Arguments::unwrap(unpacker, *obj);
    rt.push_back(obj);
  }
  return rt;
}

template <typename T>
inline WaitResult<T> Ray::wait(const std::vector<RayObject<T>> &objects, int num_objects,
                               int64_t timeout_ms) {
  auto uniqueVector = rayObject2ObjectID<T>(objects);
  auto result = _impl->wait(uniqueVector, num_objects, timeout_ms);
  return waitResultFromInernal<T>(result);
}

#include <ray/api/generated/call_funcs_impl.generated.h>

#include <ray/api/generated/create_actors_impl.generated.h>

#include <ray/api/generated/call_actors_impl.generated.h>

}  }// namespace ray::api
