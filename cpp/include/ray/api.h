
#pragma once

#include <ray/api/actor_creator.h>
#include <ray/api/actor_handle.h>
#include <ray/api/actor_task_caller.h>
#include <ray/api/exec_funcs.h>
#include <ray/api/object_ref.h>
#include <ray/api/ray_remote.h>
#include <ray/api/ray_runtime.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/task_caller.h>
#include <ray/api/wait_result.h>

#include <boost/callable_traits.hpp>
#include <memory>
#include <msgpack.hpp>

#include "ray/core.h"
namespace ray {
namespace api {

template <typename ReturnType, typename... Args>
using CreateActorFunc = ReturnType *(*)(Args...);

class Ray {
 public:
  /// Initialize Ray runtime.
  static void Init();

  /// Shutdown Ray runtime.
  static void Shutdown();

  /// Store an object in the object store.
  ///
  /// \param[in] obj The object which should be stored.
  /// \return ObjectRef A reference to the object in the object store.
  template <typename T>
  static ObjectRef<T> Put(const T &obj);

  /// Get a single object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \param[in] object The object reference which should be returned.
  /// \return shared pointer of the result.
  template <typename T>
  static std::shared_ptr<T> Get(const ObjectRef<T> &object);

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

  /// Create a `TaskCaller` for calling remote function.
  /// It is used for normal task, such as Ray::Task(Plus1, 1), Ray::Task(Plus, 1, 2).
  /// \param[in] func The function to be remote executed.
  /// \param[in] args The function arguments passed by a value or ObjectRef.
  /// \return TaskCaller.
  template <typename F, typename... Args>
  static TaskCaller<boost::callable_traits::return_type_t<F>> Task(F func, Args... args);

  /// Generic version of creating an actor
  /// It is used for creating an actor, such as: ActorCreator<Counter> creator =
  /// Ray::Actor(Counter::FactoryCreate, 1).
  template <typename ActorType, typename... Args>
  static ActorCreator<ActorType> Actor(
      CreateActorFunc<ActorType, typename FilterArgType<Args>::type...> create_func,
      Args... args);

/// TODO: The bellow specific version of creating an actor will be replaced with generic
/// version later.
#include <ray/api/generated/create_funcs.generated.h>

#include "api/generated/create_actors.generated.h"

 private:
  static std::once_flag is_inited_;

  template <typename ReturnType, typename FuncType, typename ExecFuncType,
            typename... ArgTypes>
  static TaskCaller<ReturnType> TaskInternal(FuncType &func, ExecFuncType &exec_func,
                                             ArgTypes &... args);

  template <typename ActorType, typename FuncType, typename ExecFuncType,
            typename... ArgTypes>
  static ActorCreator<ActorType> CreateActorInternal(FuncType &func,
                                                     ExecFuncType &exec_func,
                                                     ArgTypes &... args);

  /// Include the `Call` methods for calling actor methods.
  /// Used by ActorHandle to implement .Call()
  /// It is called by ActorHandle: Ray::Task(&Counter::Add, counter/*instance of
  /// Counter*/, 1);
  template <typename ReturnType, typename ActorType, typename... Args>
  static ActorTaskCaller<ReturnType> Task(
      ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
      ActorHandle<ActorType> &actor, Args... args);
};

}  // namespace api
}  // namespace ray

// --------- inline implementation ------------

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
  auto buffer = std::make_shared<msgpack::sbuffer>(Serializer::Serialize(obj));
  auto id = ray::internal::RayRuntime()->Put(buffer);
  return ObjectRef<T>(id);
}

template <typename T>
inline std::shared_ptr<T> Ray::Get(const ObjectRef<T> &object) {
  return GetFromRuntime(object);
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::Get(const std::vector<ObjectID> &ids) {
  auto result = ray::internal::RayRuntime()->Get(ids);
  std::vector<std::shared_ptr<T>> return_objects;
  return_objects.reserve(result.size());
  for (auto it = result.begin(); it != result.end(); it++) {
    auto obj = Serializer::Deserialize<std::shared_ptr<T>>((*it)->data(), (*it)->size());
    return_objects.push_back(std::move(obj));
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
  return ray::internal::RayRuntime()->Wait(ids, num_objects, timeout_ms);
}

template <typename ReturnType, typename FuncType, typename ExecFuncType,
          typename... ArgTypes>
inline TaskCaller<ReturnType> Ray::TaskInternal(FuncType &func, ExecFuncType &exec_func,
                                                ArgTypes &... args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> task_args;
  Arguments::WrapArgs(&task_args, args...);
  RemoteFunctionPtrHolder ptr;
  ptr.function_pointer = reinterpret_cast<uintptr_t>(func);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
    auto function_name = ray::internal::FunctionManager::Instance().GetFunctionName(func);
    if (function_name.empty()) {
      throw RayException(
          "Function not found. Please use RAY_REMOTE to register this function.");
    }
    ptr.function_name = std::move(function_name);
  }
  return TaskCaller<ReturnType>(ray::internal::RayRuntime().get(), ptr,
                                std::move(task_args));
}

template <typename ActorType, typename FuncType, typename ExecFuncType,
          typename... ArgTypes>
inline ActorCreator<ActorType> Ray::CreateActorInternal(FuncType &create_func,
                                                        ExecFuncType &exec_func,
                                                        ArgTypes &... args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> task_args;
  Arguments::WrapArgs(&task_args, args...);
  RemoteFunctionPtrHolder ptr;
  ptr.function_pointer = reinterpret_cast<uintptr_t>(create_func);
  ptr.exec_function_pointer = reinterpret_cast<uintptr_t>(exec_func);
  return ActorCreator<ActorType>(ray::internal::RayRuntime().get(), ptr,
                                 std::move(task_args));
}

/// Normal task.
template <typename F, typename... Args>
TaskCaller<boost::callable_traits::return_type_t<F>> Ray::Task(F func, Args... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;

  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, typename FilterArgType<Args>::type...>,
      args...);
}

/// Generic version of creating an actor.
template <typename ActorType, typename... Args>
ActorCreator<ActorType> Ray::Actor(
    CreateActorFunc<ActorType, typename FilterArgType<Args>::type...> create_func,
    Args... args) {
  return CreateActorInternal<ActorType>(
      create_func,
      CreateActorExecFunction<ActorType *, typename FilterArgType<Args>::type...>,
      args...);
}

/// TODO: The bellow specific version of creating an actor will be replaced with generic
/// version later.
#include <ray/api/generated/create_actors_impl.generated.h>

/// Actor task.
template <typename ReturnType, typename ActorType, typename... Args>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
    ActorHandle<ActorType> &actor, Args... args) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func,
      ActorExecFunction<ReturnType, ActorType, typename FilterArgType<Args>::type...>,
      actor, args...);
}

}  // namespace api
}  // namespace ray
