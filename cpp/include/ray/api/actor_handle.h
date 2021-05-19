
#pragma once

#include <ray/api/actor_task_caller.h>
#include <ray/api/arguments.h>
#include <ray/api/exec_funcs.h>
#include <ray/api/ray_runtime_holder.h>

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ActorType, typename ReturnType, typename... Args>
using ActorFunc = ReturnType (ActorType::*)(Args...);

/// A handle to an actor which can be used to invoke a remote actor method, with the
/// `Call` method.
/// \param ActorType The type of the concrete actor class.
/// Note, the `Call` method is defined in actor_call.generated.h.
template <typename ActorType>
class ActorHandle {
 public:
  ActorHandle();

  ActorHandle(const ActorID &id);

  /// Get a untyped ID of the actor
  const ActorID &ID() const;

  /// Include the `Call` methods for calling remote functions.
  template <typename F>
  ActorTaskCaller<F> Task(F actor_func);

  /// Make ActorHandle serializable
  MSGPACK_DEFINE(id_);

 private:
  ActorID id_;
};

// ---------- implementation ----------
template <typename ActorType>
ActorHandle<ActorType>::ActorHandle() {}

template <typename ActorType>
ActorHandle<ActorType>::ActorHandle(const ActorID &id) {
  id_ = id;
}

template <typename ActorType>
const ActorID &ActorHandle<ActorType>::ID() const {
  return id_;
}

template <typename ActorType>
template <typename F>
ActorTaskCaller<F> ActorHandle<ActorType>::Task(F actor_func) {
  RemoteFunctionHolder remote_func_holder(actor_func);
  return ActorTaskCaller<F>(internal::RayRuntime().get(), id_, remote_func_holder);
}

}  // namespace api
}  // namespace ray
