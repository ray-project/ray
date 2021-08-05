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

#include <ray/api/actor_handle.h>

namespace ray {
namespace api {

template <typename F>
using GetActorType = std::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(RayRuntime *runtime, RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ActorHandle<GetActorType<F>> Remote(Args &&... args);

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args &&... args) {
  StaticCheck<F, Args...>();
  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_actor_id = runtime_->CreateActor(remote_function_holder_, args_);
  return ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
