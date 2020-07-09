
#pragma once

#include <memory>

#include <ray/api/generated/actor_funcs.generated.h>
#include <ray/api/generated/create_funcs.generated.h>
#include <ray/api/generated/funcs.generated.h>
#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>
#include "ray/core.h"
namespace ray {
namespace api {

template <typename T>
class ObjectRef;
template <typename T>
class ActorHandle;
template <typename ReturnType>
class TaskCaller;

template <typename ReturnType>
class ActorTaskCaller;

template <typename ActorType>
class ActorCreator;

class WaitResult;

class Ray {
 public:
  /// Initialize Ray runtime.
  static void Init();

  /// Store an object in the object store.
  ///
  /// \param[in] obj The object which should be stored.
  /// \return ObjectRef A reference to the object in the object store.
  template <typename T>
  static ObjectRef<T> Put(const T &obj);

  /// Get a list of objects from the object store.
  /// This method will be blocked until all the objects are ready.
  ///
  /// \param[in] ids The object id array which should be got.
  /// \return shared pointer array of the result.
  template <typename T>
  static std::vector<std::shared_ptr<T>> Get(const std::vector<ObjectID> &ids);

  /// Get a list of objects from the object store.
  /// This method will be blocked until all the objects are ready.
  ///
  /// \param[in] objects The object array which should be got.
  /// \return shared pointer array of the result.
  template <typename T>
  static std::vector<std::shared_ptr<T>> Get(const std::vector<ObjectRef<T>> &ids);

  /// Wait for a list of objects to be locally available,
  /// until specified number of objects are ready, or specified timeout has passed.
  ///
  /// \param[in] ids The object id array which should be waited.
  /// \param[in] num_objects The minimum number of objects to wait.
  /// \param[in] timeout_ms The maximum wait time in milliseconds.
  /// \return Two arrays, one containing locally available objects, one containing the
  /// rest.
  static WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                         int timeout_ms);

/// Include the `Call` methods for calling remote functions.
#include "api/generated/call_funcs.generated.h"

/// Include the `Actor` methods for creating actors.
#include "api/generated/create_actors.generated.h"

 private:
  static RayRuntime *runtime_;

  static std::once_flag is_inited_;

  /// Used by ObjectRef to implement .Get()
  template <typename T>
  static std::shared_ptr<T> Get(const ObjectRef<T> &object);

  template <typename ReturnType, typename FuncType, typename ExecFuncType,
            typename... ArgTypes>
  static TaskCaller<ReturnType> TaskInternal(FuncType &func, ExecFuncType &exec_func,
                                             ArgTypes &... args);

  template <typename ActorType, typename FuncType, typename ExecFuncType,
            typename... ArgTypes>
  static ActorCreator<ActorType> CreateActorInternal(FuncType &func,
                                                     ExecFuncType &exec_func,
                                                     ArgTypes &... args);

  template <typename ReturnType, typename ActorType, typename FuncType,
            typename ExecFuncType, typename... ArgTypes>
  static ActorTaskCaller<ReturnType> CallActorInternal(FuncType &actor_func,
                                                       ExecFuncType &exec_func,
                                                       ActorHandle<ActorType> &actor,
                                                       ArgTypes &... args);

/// Include the `Call` methods for calling actor methods.
/// Used by ActorHandle to implement .Call()
#include "api/generated/call_actors.generated.h"

  template <typename T>
  friend class ObjectRef;

  template <typename ActorType>
  friend class ActorHandle;
};

}  // namespace api
}  // namespace ray

// --------- inline implementation ------------
#include <ray/api/actor_creator.h>
#include <ray/api/actor_handle.h>
#include <ray/api/actor_task_caller.h>
#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/serializer.h>
#include <ray/api/task_caller.h>
#include <ray/api/wait_result.h>

namespace ray {
namespace api {

template <typename T>
inline static std::vector<ObjectID> ObjectRefsToObjectIDs(
    const std::vector<ObjectRef<T>> &object_refs) {
  std::vector<ObjectID> object_ids;
  for (auto it = object_refs.begin(); it != object_refs.end(); it++) {
    object_ids.push_back(it->ID());
  }
  return object_ids;
}

template <typename T>
inline ObjectRef<T> Ray::Put(const T &obj) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Serializer::Serialize(packer, obj);
  auto id = runtime_->Put(buffer);
  return ObjectRef<T>(id);
}

template <typename T>
inline std::shared_ptr<T> Ray::Get(const ObjectRef<T> &object) {
  auto packed_object = runtime_->Get(object.ID());
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(packed_object->size());
  memcpy(unpacker.buffer(), packed_object->data(), packed_object->size());
  unpacker.buffer_consumed(packed_object->size());
  std::shared_ptr<T> return_object(new T);
  Serializer::Deserialize(unpacker, return_object.get());
  return return_object;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::Get(const std::vector<ObjectID> &ids) {
  auto result = runtime_->Get(ids);
  std::vector<std::shared_ptr<T>> return_objects;
  return_objects.reserve(result.size());
  for (auto it = result.begin(); it != result.end(); it++) {
    msgpack::unpacker unpacker;
    unpacker.reserve_buffer((*it)->size());
    memcpy(unpacker.buffer(), (*it)->data(), (*it)->size());
    unpacker.buffer_consumed((*it)->size());
    std::shared_ptr<T> obj(new T);
    Serializer::Deserialize(unpacker, obj.get());
    return_objects.push_back(obj);
  }
  return return_objects;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::Get(const std::vector<ObjectRef<T>> &ids) {
  auto object_ids = ObjectRefsToObjectIDs<T>(ids);
  return Get<T>(object_ids);
}

inline WaitResult Ray::Wait(const std::vector<ObjectID> &ids, int num_objects,
                            int timeout_ms) {
  return runtime_->Wait(ids, num_objects, timeout_ms);
}

template <typename ReturnType, typename FuncType, typename ExecFuncType,
          typename... ArgTypes>
inline TaskCaller<ReturnType> Ray::TaskInternal(FuncType &func, ExecFuncType &exec_func,
                                                ArgTypes &... args) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::WrapArgs(packer, args...);
  RemoteFunctionPtrHolder ptr;
  ptr.function_pointer = reinterpret_cast<uintptr_t>(func);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  return TaskCaller<ReturnType>(runtime_, ptr, buffer);
}

template <typename ActorType, typename FuncType, typename ExecFuncType,
          typename... ArgTypes>
inline ActorCreator<ActorType> Ray::CreateActorInternal(FuncType &create_func,
                                                        ExecFuncType &exec_func,
                                                        ArgTypes &... args) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::WrapArgs(packer, args...);
  RemoteFunctionPtrHolder ptr;
  ptr.function_pointer = reinterpret_cast<uintptr_t>(create_func);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  return ActorCreator<ActorType>(runtime_, ptr, buffer);
}

template <typename ReturnType, typename ActorType, typename FuncType,
          typename ExecFuncType, typename... ArgTypes>
inline ActorTaskCaller<ReturnType> Ray::CallActorInternal(FuncType &actor_func,
                                                          ExecFuncType &exec_func,
                                                          ActorHandle<ActorType> &actor,
                                                          ArgTypes &... args) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::WrapArgs(packer, args...);
  RemoteFunctionPtrHolder ptr;
  MemberFunctionPtrHolder holder = *(MemberFunctionPtrHolder *)(&actor_func);
  ptr.function_pointer = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  return ActorTaskCaller<ReturnType>(runtime_, actor.ID(), ptr, buffer);
}

#include <ray/api/generated/exec_funcs.generated.h>

#include <ray/api/generated/call_funcs_impl.generated.h>

#include <ray/api/generated/create_actors_impl.generated.h>

#include <ray/api/generated/call_actors_impl.generated.h>

}  // namespace api
}  // namespace ray
