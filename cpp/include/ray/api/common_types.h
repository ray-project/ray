// Copyright 2017 The Ray Authors.
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
#include <memory>
#include <msgpack.hpp>
#include <string_view>

#include "boost/optional.hpp"

namespace ray {
namespace internal {

struct TaskArg {
  TaskArg() = default;
  TaskArg(TaskArg &&rhs) {
    buf = std::move(rhs.buf);
    id = rhs.id;
    meta_str = std::move(rhs.meta_str);
  }

  TaskArg(const TaskArg &) = delete;
  TaskArg &operator=(TaskArg const &) = delete;
  TaskArg &operator=(TaskArg &&) = delete;

  /// If the buf is initialized shows it is a value argument.
  boost::optional<msgpack::sbuffer> buf;
  /// If the id is initialized shows it is a reference argument.
  boost::optional<std::string> id;

  std::string_view meta_str;
};

}  // namespace internal
}  // namespace ray