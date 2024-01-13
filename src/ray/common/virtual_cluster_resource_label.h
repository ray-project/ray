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

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/task/task_common.h"

namespace ray {

struct VirtualClusterResourceLabel {
  std::string original_resource;
  VirtualClusterID vc_id;

  // Parses from "CPU_vc_vchex" to
  // {.original_resource = "CPU", .vc_id = VirtualClusterID::FromHex("vchex")}
  static std::optional<VirtualClusterResourceLabel> Parse(const std::string &resource);

  // Format into a string, from
  // {.original_resource = "CPU", .vc_id = VirtualClusterID::FromHex("vchex")}
  // to "CPU_vc_vehex".
  std::string Format();
};

}  // namespace ray
