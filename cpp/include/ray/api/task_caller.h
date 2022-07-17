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

#include <ray/api/static_check.h>
#include <ray/api/task_options.h>
namespace ray {
namespace internal {

template <typename F>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionHolder remote_function_holder);

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&...args);

  TaskCaller &SetName(std::string name) {
    task_options_.name = std::move(name);
    return *this;
  }

  TaskCaller &SetResources(std::unordered_map<std::string, double> resources) {
    task_options_.resources = std::move(resources);
    return *this;
  }

  TaskCaller &SetResource(std::string name, double value) {
    task_options_.resources.emplace(std::move(name), value);
    return *this;
  }

  TaskCaller &SetPlacementGroup(PlacementGroup group, int bundle_index) {
    task_options_.group = group;
    task_options_.bundle_index = bundle_index;
    return *this;
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_{};
  std::string function_name_;
  std::vector<TaskArg> args_;
  CallOptions task_options_;
};

// ---------- implementation ----------

template <typename F>
TaskCaller<F>::TaskCaller() {}

template <typename F>
TaskCaller<F>::TaskCaller(RayRuntime *runtime,
                          RemoteFunctionHolder remote_function_holder)
    : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> TaskCaller<F>::Remote(
    Args &&...args) {
  CheckTaskOptions(task_options_.resources);

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

  auto returned_object_id = runtime_->Call(remote_function_holder_, args_, task_options_);
  using ReturnType = boost::callable_traits::return_type_t<F>;
  auto return_ref = ObjectRef<ReturnType>(returned_object_id);
  // The core worker will add an initial ref to each return ID to keep it in
  // scope. Now that we've created the frontend ObjectRef, remove this initial
  // ref.
  runtime_->RemoveLocalReference(returned_object_id);
  return return_ref;
}
}  // namespace internal
}  // namespace ray
