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
#include <ray/api/task_options.h>

namespace ray {
namespace internal {

template <typename F>
using GetActorType = std::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(RayRuntime *runtime, RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ray::ActorHandle<GetActorType<F>, is_x_lang_v<F>> Remote(Args &&...args);

  ActorCreator &SetName(std::string name) {
    create_options_.name = std::move(name);
    return *this;
  }

  ActorCreator &SetResources(std::unordered_map<std::string, double> resources) {
    create_options_.resources = std::move(resources);
    return *this;
  }

  ActorCreator &SetResource(std::string name, double value) {
    create_options_.resources.emplace(std::move(name), value);
    return *this;
  }

  ActorCreator &SetMaxRestarts(int max_restarts) {
    create_options_.max_restarts = max_restarts;
    return *this;
  }

  ActorCreator &SetMaxConcurrency(int max_concurrency) {
    create_options_.max_concurrency = max_concurrency;
    return *this;
  }

  ActorCreator &SetPlacementGroup(PlacementGroup group, int bundle_index) {
    create_options_.group = group;
    create_options_.bundle_index = bundle_index;
    return *this;
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<TaskArg> args_;
  ActorCreationOptions create_options_{};
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ActorHandle<GetActorType<F>, is_x_lang_v<F>> ActorCreator<F>::Remote(Args &&...args) {
  CheckTaskOptions(create_options_.resources);

  if constexpr (is_x_lang_v<F>) {
    using ArgsTuple = std::tuple<Args...>;
    Arguments::WrapArgs<ArgsTuple>(remote_function_holder_.lang_type,
                                   &args_,
                                   std::make_index_sequence<sizeof...(Args)>{},
                                   std::forward<Args>(args)...);
  } else {
    StaticCheck<F, Args...>();
    using ArgsTuple = RemoveReference_t<boost::callable_traits::args_t<F>>;
    Arguments::WrapArgs<ArgsTuple>(remote_function_holder_.lang_type,
                                   &args_,
                                   std::make_index_sequence<sizeof...(Args)>{},
                                   std::forward<Args>(args)...);
  }

  auto returned_actor_id =
      runtime_->CreateActor(remote_function_holder_, args_, create_options_);
  return ActorHandle<GetActorType<F>, is_x_lang_v<F>>(returned_actor_id);
}
}  // namespace internal
}  // namespace ray
