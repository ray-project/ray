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

#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/util/compat.h"

namespace ray {

using ProcessEnvironment = absl::flat_hash_map<std::string, std::string>;

using StartupToken = int64_t;

/**
 * @class ProcessInterface
 * @details This interface is used to abstract the process implementation
 *          and provide easy injection of fake process for testing.
 *          The process implementation is used to track the lifetime of the
 *          underlying OS process, and provides wrappers to the system calls
 *          to interact with the process.
 */
class ProcessInterface {
 public:
  virtual ~ProcessInterface() = default;

  /**
   * @brief Get the process ID.
   * @return The process ID, or -1 for a null process.
   */
  virtual pid_t GetId() const = 0;

  /**
   * @brief Check if this is a null process object.
   * @return True if the process is null, false otherwise.
   */
  virtual bool IsNull() const = 0;

  /**
   * @brief Check if this process has a valid (non-negative) PID.
   * @return True if the process is valid, false otherwise.
   */
  virtual bool IsValid() const = 0;

  /**
   * @brief Forcefully kills the process.
   * @details It is unsafe to kill unowned processes (processes created outside of raylet)
   *          as their death may not be tracked by the parent process and can result
   *          in double kill attempts.
   */
  virtual void Kill() = 0;

  /**
   * @brief Check whether the process is alive.
   * @return True if the process is alive, false otherwise.
   */
  virtual bool IsAlive() const = 0;

  /**
   * @brief Waits for process to terminate.
   * @details Not supported for unowned processes.
   * @return The process's exit code. Returns -1 for a null process.
   */
  virtual int Wait() const = 0;
};

}  // namespace ray
