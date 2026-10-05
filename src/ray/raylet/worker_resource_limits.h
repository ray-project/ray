// Copyright 2026 The Ray Authors.
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

#include <string>

#include "ray/common/scheduling/resource_set.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

namespace ray {
namespace raylet {

/// Build a normalized per-worker limit profile from resources assigned to a lease.
/// Placement group aliases are deduplicated. An empty profile is returned when the
/// feature is disabled or the runtime environment is not an image_uri environment.
rpc::WorkerResourceLimits BuildWorkerResourceLimits(
    const std::string &serialized_runtime_env, const ResourceSet &resources);

bool WorkerResourceLimitsEqual(const rpc::WorkerResourceLimits &left,
                               const rpc::WorkerResourceLimits &right);

bool HasWorkerResourceLimits(const rpc::WorkerResourceLimits &limits);

}  // namespace raylet
}  // namespace ray
