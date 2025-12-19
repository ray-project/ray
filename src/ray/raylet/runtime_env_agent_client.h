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

#include <boost/asio/deadline_timer.hpp>
#include <csignal>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/public/runtime_environment.pb.h"

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

// Interface to talk to RuntimeEnvManager.
class RuntimeEnvAgentClient {
 public:
  // Creates a concrete Client that can make HTTP requests to address:port.
  // Retries all requests every `agent_manager_retry_interval_ms` on NotFound.
  static std::unique_ptr<RuntimeEnvAgentClient> Create(
      instrumented_io_context &io_context,
      const std::string &address,
      int port,
      // Not using typedef to avoid conflict with agent_manager.h
      std::function<std::shared_ptr<boost::asio::deadline_timer>(
          std::function<void()>, uint32_t delay_ms)> delay_executor,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
      uint32_t agent_register_timeout_ms =
          RayConfig::instance().agent_register_timeout_ms(),
      uint32_t agent_manager_retry_interval_ms =
          RayConfig::instance().agent_manager_retry_interval_ms());

  virtual ~RuntimeEnvAgentClient() = default;

  /// Request agent to increase the runtime env reference. This API is not idempotent. The
  /// client automatically retries on network errors.
  /// \param[in] job_id The job id which the runtime env belongs to.
  /// \param[in] serialized_runtime_env The runtime
  /// environment serialized in JSON as from `RuntimeEnv::Serialize` method.
  /// \param[in] callback The callback function.
  virtual void GetOrCreateRuntimeEnv(const JobID &job_id,
                                     const std::string &serialized_runtime_env,
                                     const rpc::RuntimeEnvConfig &runtime_env_config,
                                     GetOrCreateRuntimeEnvCallback callback) = 0;

  /// Request agent to decrease the runtime env reference. This API is not idempotent. The
  /// client automatically retries on network errors.
  /// \param[in] serialized_runtime_env The runtime environment serialized in JSON as from
  /// `RuntimeEnv::Serialize` method.
  /// \param[in] callback The callback function.
  virtual void DeleteRuntimeEnvIfPossible(
      const std::string &serialized_runtime_env,
      DeleteRuntimeEnvIfPossibleCallback callback) = 0;

  // NOTE: The service has another method `GetRuntimeEnvsInfo` but nobody in raylet uses
  // it.
};

}  // namespace raylet
}  // namespace ray
