// Copyright 2019-2020 The Ray Authors.
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

#include "src/ray/protobuf/common.pb.h"

namespace ray {

// NOTE(hchen): Below we alias `ray::rpc::Language|TaskType)` in  `ray` namespace.
// The reason is because other code should use them as if they were defined in this
// `task_common.h` file, shouldn't care about the implementation detail that they
// are defined in protobuf.

/// See `common.proto` for definition of `Language` enum.
using Language = rpc::Language;
/// See `common.proto` for definition of `TaskType` enum.
using TaskType = rpc::TaskType;

}  // namespace ray
