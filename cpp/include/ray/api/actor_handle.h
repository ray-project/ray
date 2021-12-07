// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api/actor_task_caller.h>
#include <ray/api/ray_runtime_holder.h>

namespace ray {

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
  ray::internal::ActorTaskCaller<F> Task(F actor_func);

  void Kill();
  void Kill(bool no_restart);

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
ray::internal::ActorTaskCaller<F> ActorHandle<ActorType>::Task(F actor_func) {
  static_assert(
      std::is_member_function_pointer_v<F>,
      "Incompatible type: non-member function cannot be called with Actor::Task.");
  using Self = boost::callable_traits::class_of_t<F>;
  static_assert(
      std::is_same<ActorType, Self>::value || std::is_base_of<Self, ActorType>::value,
      "Class types must be same.");
  ray::internal::RemoteFunctionHolder remote_func_holder(actor_func);
  return ray::internal::ActorTaskCaller<F>(internal::GetRayRuntime().get(), id_,
                                           std::move(remote_func_holder));
}

template <typename ActorType>
void ActorHandle<ActorType>::Kill() {
  ray::internal::GetRayRuntime()->KillActor(id_, true);
}

template <typename ActorType>
void ActorHandle<ActorType>::Kill(bool no_restart) {
  ray::internal::GetRayRuntime()->KillActor(id_, no_restart);
}

}  // namespace ray
