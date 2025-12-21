// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "ray/util/process_interface.h"

namespace ray {

/// \class ProcessFactory
///
/// Factory implementation that creates real Process objects.
/// Use this in production code.
class ProcessFactory {
 public:
  /**
   * Creates a null process object.
   *
   * @return A unique pointer to a null process object.
   */
  static std::unique_ptr<ProcessInterface> CreateNull();

  /**
   * Creates a fake process for testing
   *
   * @return A unique pointer to the fake process object.
   */
  static std::unique_ptr<ProcessInterface> CreateNewDummy();

  /**
   * Creates a process object from an existing PID.
   *
   * @param pid The PID of the process to create.
   * @return A unique pointer to the process object.
   */
  static std::unique_ptr<ProcessInterface> FromPid(pid_t pid);

  /**
   * Creates a new process with full control over all parameters.
   *
   * @param argv The command-line of the process to spawn (terminated with NULL).
   * @param io_service Boost.Asio I/O service (optional, can be nullptr).
   * @param ec Returns any error that occurred when spawning the process.
   * @param decouple True iff the parent will not wait for the child to exit.
   * @param env Additional environment variables.
   * @param pipe_to_stdin If true, creates a pipe to child's stdin for health checking.
   * @param add_to_cgroup_hook Hook called after fork/before exec to move process to
   * cgroup.
   * @param new_process_group Whether to create a new process group.
   * @return A unique pointer to the process object.
   */
  static std::unique_ptr<ProcessInterface> Create(
      const char *argv[],
      void *io_service,
      std::error_code &ec,
      bool decouple = false,
      const ProcessEnvironment &env = {},
      bool pipe_to_stdin = false,
      std::function<void(const std::string &)> add_to_cgroup_hook =
          [](const std::string &) {},
      bool new_process_group = false);

  /**
   * Convenience function to start a process in the background.
   *
   * @param args The command-line arguments.
   * @param decouple True iff the parent will not wait for the child to exit.
   * @param pid_file A file to write the PID of the spawned process in.
   * @param env Additional environment variables.
   * @param new_process_group Whether to create a new process group.
   * @return A pair of the process object and any error that occurred.
   */
  static std::pair<std::unique_ptr<ProcessInterface>, std::error_code> Spawn(
      const std::vector<std::string> &args,
      bool decouple,
      const std::string &pid_file = std::string(),
      const ProcessEnvironment &env = {},
      bool new_process_group = false);

  /**
   * Convenience function to run the given command line and wait for it to finish.
   *
   * @param args The command-line arguments.
   * @param env Additional environment variables.
   * @return Any error that occurred.
   */
  static std::error_code Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env = {});
};

}  // namespace ray
