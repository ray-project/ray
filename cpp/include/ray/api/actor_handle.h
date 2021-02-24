
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

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

  template <typename ReturnType, typename... Args>
  ActorTaskCaller<ReturnType> Task(
      ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
      Args... args);

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
template <typename ReturnType, typename... Args>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc<ActorType, ReturnType, typename FilterArgType<Args>::type...> actor_func,
    Args... args) {
  return Ray::Task(actor_func, *this, args...);
}

}  // namespace api
}  // namespace ray
