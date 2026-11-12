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

/**
 * @brief Build a normalized per-worker limit profile, deduplicating placement group
 * aliases.
 * @param serialized_runtime_env JSON-serialized runtime environment for the lease.
 * @param resources Resources assigned to the lease.
 * @return Normalized limits, including a validation error for unrepresentable requests.
 * Returns an empty profile when the feature is disabled or the runtime environment
 * does not specify a nonempty image_uri.
 */
rpc::WorkerResourceLimits BuildWorkerResourceLimits(
    const std::string &serialized_runtime_env, const ResourceSet &resources);

/**
 * @brief Compare worker resource profiles for worker reuse.
 * @param left First profile.
 * @param right Second profile.
 * @return Whether all limits and validation errors match.
 */
bool WorkerResourceLimitsEqual(const rpc::WorkerResourceLimits &left,
                               const rpc::WorkerResourceLimits &right);

/**
 * @brief Check whether a profile requires per-worker handling.
 * @param limits Profile to inspect.
 * @return Whether any limit or validation error is set.
 */
bool HasWorkerResourceLimits(const rpc::WorkerResourceLimits &limits);

}  // namespace raylet
}  // namespace ray
