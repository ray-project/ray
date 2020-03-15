
#pragma once

#include <memory>

#include <ray/api/generated/actor_funcs.generated.h>
#include <ray/api/generated/create_funcs.generated.h>
#include <ray/api/generated/funcs.generated.h>
#include <ray/api/ray_runtime.h>
#include <ray/core.h>
#include <msgpack.hpp>

/**
 * ray api definition
 *
 */
namespace ray {
namespace api {

template <typename T>
class RayObject;
template <typename T>
class RayActor;

class WaitResult;

class Ray {
  template <typename T>
  friend class RayObject;

 public:
  /// Initialize Ray runtime with the default runtime implementation.
  static void Init();

  /// Store an object in the object store.
  ///
  /// \param[in] obj The object which should be stored.
  /// \return RayObject which like a future and provide a 'Get' method.
  template <typename T>
  static RayObject<T> Put(const T &obj);

  /// Get a list of objects from the object store.
  /// This method will be blocked until all the objects ready.
  ///
  /// \param[in] ids The object id array which should be got.
  /// \return shared pointer array of the result.
  template <typename T>
  static std::vector<std::shared_ptr<T>> Get(const std::vector<ObjectID> &ids);

  /// Get a list of objects from the object store.
  /// This method will be blocked until all the objects ready.
  ///
  /// \param[in] objects The object array which should be got.
  /// \return shared pointer array of the result.
  template <typename T>
  static std::vector<std::shared_ptr<T>> Get(const std::vector<RayObject<T>> &objects);

  /// Wait for a list of RayObjects to be locally available,
  /// until specified number of objects are ready, or specified timeout has passed.
  ///
  /// \param[in] ids The object id array which should be waited.
  /// \param[in] num_objects The minimum number of objects to wait.
  /// \param[in] timeout_ms The maximum wait time.
  /// \return Two arrays, one containing locally available objects, one containing the
  /// rest.
  static WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                         int64_t timeout_ms);

/// Include all the Call method which should be auto genrated.
/// Call a general remote fucntion.
///
/// \param[in] func The function pointer to be remote execution.
/// \param[in] arg The function args.
/// \return RayObject.
#include "api/generated/call_funcs.generated.h"

/// Include all the Call method which should be auto genrated.
/// Call a factory method which create an actor and return an actor pointer.
///
/// \param[in] func The function pointer to be remote execution.
/// \param[in] arg The function args.
/// \return RayActor.
#include "api/generated/create_actors.generated.h"

/// Include all the Call method which should be auto genrated.
/// Call a actor remote fucntion.
///
/// \param[in] func The function pointer to be remote execution.
/// \param[in] arg The function args.
/// \return RayObject.
#include "api/generated/call_actors.generated.h"

 private:
  static RayRuntime *_impl;

  /// Used by RayObject to implement .Get()
  template <typename T>
  static std::shared_ptr<T> Get(const RayObject<T> &object);
};

}  // namespace api
}  // namespace ray

// --------- inline implementation ------------
#include <ray/api/arguments.h>
#include <ray/api/ray_actor.h>
#include <ray/api/ray_object.h>
#include <ray/api/wait_result.h>

namespace ray {
namespace api {

template <typename T>
inline static std::vector<ObjectID> RayObject2ObjectID(
    const std::vector<RayObject<T>> &rayObjects) {
  std::vector<ObjectID> unqueIds;
  for (auto it = rayObjects.begin(); it != rayObjects.end(); it++) {
    unqueIds.push_back(it->ID());
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
inline RayObject<T> Ray::Put(const T &obj) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, obj);
  auto id = _impl->Put(buffer);
  return RayObject<T>(id);
}

template <typename T>
inline std::shared_ptr<T> Ray::Get(const RayObject<T> &object) {
  auto data = _impl->Get(object.ID());
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(data->size());
  memcpy(unpacker.buffer(), data->data(), data->size());
  unpacker.buffer_consumed(data->size());
  std::shared_ptr<T> rt(new T);
  Arguments::Unwrap(unpacker, *rt);
  return rt;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::Get(const std::vector<ObjectID> &ids) {
  auto result = _impl->Get(ids);
  std::vector<std::shared_ptr<T>> rt;
  for (auto it = result.begin(); it != result.end(); it++) {
    msgpack::unpacker unpacker;
    unpacker.reserve_buffer((*it)->size());
    memcpy(unpacker.buffer(), (*it)->data(), (*it)->size());
    unpacker.buffer_consumed((*it)->size());
    std::shared_ptr<T> obj(new T);
    Arguments::Unwrap(unpacker, *obj);
    rt.push_back(obj);
  }
  return rt;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::Get(
    const std::vector<RayObject<T>> &objects) {
  auto uniqueVector = RayObject2ObjectID<T>(objects);
  return Get<T>(uniqueVector);
}

inline WaitResult Ray::Wait(const std::vector<ObjectID> &ids, int num_objects,
                            int64_t timeout_ms) {
  return _impl->Wait(ids, num_objects, timeout_ms);
}

#include <ray/api/generated/exec_funcs.generated.h>

#include <ray/api/generated/call_funcs_impl.generated.h>

#include <ray/api/generated/create_actors_impl.generated.h>

#include <ray/api/generated/call_actors_impl.generated.h>

}  // namespace api
}  // namespace ray
