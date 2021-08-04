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

#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/static_check.h>

namespace ray {
namespace api {

template <typename F>
class ActorTaskCaller {
 public:
  ActorTaskCaller() = default;

  ActorTaskCaller(RayRuntime *runtime, std::string id,
                  RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime),
        id_(id),
        remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  RayRuntime *runtime_;
  std::string id_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> ActorTaskCaller<F>::Remote(
    Args &&... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;
  StaticCheck<F, Args...>();

  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->CallActor(remote_function_holder_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}

}  // namespace api
}  // namespace ray
