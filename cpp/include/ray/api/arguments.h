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

#include <ray/api/object_ref.h>
#include <ray/api/serializer.h>

#include <msgpack.hpp>

namespace ray {
namespace internal {

/// Check T is ObjectRef or not.
template <typename T>
struct is_object_ref : std::false_type {};

template <typename T>
struct is_object_ref<ObjectRef<T>> : std::true_type {};

class Arguments {
 public:
  template <typename ArgType>
  static void WrapArgsImpl(std::vector<TaskArg> *task_args, ArgType &&arg) {
    static_assert(!is_object_ref<ArgType>::value, "ObjectRef can not be wrapped");

    msgpack::sbuffer buffer = Serializer::Serialize(arg);
    TaskArg task_arg;
    task_arg.buf = std::move(buffer);
    /// Pass by value.
    task_args->emplace_back(std::move(task_arg));
  }

  template <typename ArgType>
  static void WrapArgsImpl(std::vector<TaskArg> *task_args, ObjectRef<ArgType> &arg) {
    /// Pass by reference.
    TaskArg task_arg{};
    task_arg.id = arg.ID();
    task_args->emplace_back(std::move(task_arg));
  }

  template <typename... OtherArgTypes>
  static void WrapArgs(std::vector<TaskArg> *task_args, OtherArgTypes &&... args) {
    (void)std::initializer_list<int>{
        (WrapArgsImpl(task_args, std::forward<OtherArgTypes>(args)), 0)...};
    /// Silence gcc warning error.
    (void)task_args;
  }
};

}  // namespace internal
}  // namespace ray