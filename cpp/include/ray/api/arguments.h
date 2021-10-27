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
#include <ray/api/type_traits.h>

#include <msgpack.hpp>

namespace ray {
namespace internal {

class Arguments {
 public:
  template <typename OriginArgType, typename InputArgTypes>
  static void WrapArgsImpl(std::vector<TaskArg> *task_args, InputArgTypes &&arg) {
    if constexpr (is_object_ref_v<OriginArgType> || is_object_ref_v<InputArgTypes>) {
      /// Pass by reference.
      TaskArg task_arg{};
      task_arg.id = arg.ID();
      task_args->emplace_back(std::move(task_arg));
    } else {
      msgpack::sbuffer buffer = Serializer::Serialize(arg);
      TaskArg task_arg;
      task_arg.buf = std::move(buffer);
      /// Pass by value.
      task_args->emplace_back(std::move(task_arg));
    }
  }

  template <typename OriginArgsTuple, size_t... I, typename... InputArgTypes>
  static void WrapArgs(std::vector<TaskArg> *task_args, std::index_sequence<I...>,
                       InputArgTypes &&...args) {
    (void)std::initializer_list<int>{
        (WrapArgsImpl<std::tuple_element_t<I, OriginArgsTuple>>(
             task_args, std::forward<InputArgTypes>(args)),
         0)...};
    /// Silence gcc warning error.
    (void)task_args;
  }
};

}  // namespace internal
}  // namespace ray