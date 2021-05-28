
#pragma once

#include <ray/api/actor_task_caller.h>
#include <ray/api/ray_runtime_holder.h>

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

  ActorHandle(const std::string &id);

  /// Get a untyped ID of the actor
  const std::string &ID() const;

  /// Include the `Call` methods for calling remote functions.
  template <typename F>
  ActorTaskCaller<F> Task(F actor_func);

  /// Make ActorHandle serializable
  MSGPACK_DEFINE(id_);

 private:
  std::string id_;
};

// ---------- implementation ----------
template <typename ActorType>
ActorHandle<ActorType>::ActorHandle() {}

template <typename ActorType>
ActorHandle<ActorType>::ActorHandle(const std::string &id) {
  id_ = id;
}

template <typename ActorType>
const std::string &ActorHandle<ActorType>::ID() const {
  return id_;
}

template <typename ActorType>
template <typename F>
ActorTaskCaller<F> ActorHandle<ActorType>::Task(F actor_func) {
  using Self = boost::callable_traits::class_of_t<F>;
  static_assert(
      std::is_same<ActorType, Self>::value || std::is_base_of<Self, ActorType>::value,
      "class types must be same");
  RemoteFunctionHolder remote_func_holder(actor_func);
  return ActorTaskCaller<F>(internal::RayRuntime().get(), id_,
                            std::move(remote_func_holder));
}

}  // namespace api
}  // namespace ray
