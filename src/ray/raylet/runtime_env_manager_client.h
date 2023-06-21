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

#include <csignal>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/rpc/runtime_env/runtime_env_client.h"

namespace ray {
namespace raylet {

/// Callback that's called after runtime env is created.
/// \param[in] successful Whether or not the creation was successful.
/// \param[in] serialized_runtime_env_context Serialized context.
/// \param[in] setup_error_message The error message if runtime env creation fails.
/// It must be only set when successful == false.
using GetOrCreateRuntimeEnvCallback =
    std::function<void(bool successful,
                       const std::string &serialized_runtime_env_context,
                       const std::string &setup_error_message)>;
using DeleteRuntimeEnvIfPossibleCallback = std::function<void(bool successful)>;

class RuntimeEnvManagerClient {
 public:
  // Creates a concrete Client as a thin wrapper of
  // `rpc::RuntimeEnvAgentClientInterface`, translating arguments.
  static std::shared_ptr<RuntimeEnvManagerClient> Create(
      std::shared_ptr<rpc::RuntimeEnvAgentClientInterface> runtime_env_agent_client);

  virtual ~RuntimeEnvManagerClient() {}

  /// Request agent to increase the runtime env reference. This API is not idempotent.
  /// \param[in] job_id The job id which the runtime env belongs to.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] serialized_allocated_resource_instances The serialized allocated
  /// resource instances. \param[in] callback The callback function.
  virtual void GetOrCreateRuntimeEnv(
      const JobID &job_id,
      const std::string &serialized_runtime_env,
      const rpc::RuntimeEnvConfig &runtime_env_config,
      const std::string &serialized_allocated_resource_instances,
      GetOrCreateRuntimeEnvCallback callback);

  /// Request agent to decrease the runtime env reference. This API is not idempotent.
  /// \param[in] serialized_runtime_env The serialized runtime environment.
  /// \param[in] callback The callback function.
  virtual void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                          DeleteRuntimeEnvIfPossibleCallback callback);

  // NOTE: The service has another method `GetRuntimeEnvsInfo` but nobody in raylet uses
  // it.
};

}  // namespace raylet
}  // namespace ray