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

#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "ray/util/compat.h"
#include "ray/util/logging.h"
#include "ray/util/process_interface.h"

// TODO(#54703): Put this type in a separate target.
using AddProcessToCgroupHook = std::function<void(const std::string &)>;

namespace ray {

class Process : public ProcessInterface {
 public:
#ifndef PID_MAX_LIMIT
  // This is defined by Linux to be the maximum allowable number of processes
  // There's no guarantee for other OSes, but it's useful for testing purposes.
  static constexpr int PID_MAX_LIMIT = 1 << 22;
#endif

  ~Process();

  /**
   * @brief Creates a null process object. Two null process objects are assumed equal.
   */
  Process();

  /**
   * @brief Creates a new process.
   * @param[in] argv The command-line of the process to spawn (terminated with NULL).
   * @param[in] ec Returns any error that occurred when spawning the process.
   * @param[in] decouple True iff the parent will not wait for the child to exit.
   * @param[in] env Additional environment variables to be set on this process besides
   *            the environment variables of the parent process.
   * @param[in] pipe_to_stdin If true, it creates a pipe and redirect to child process'
   *            stdin. It is used for health checking from a child process.
   *            Child process can read stdin to detect when the current process dies.
   * @param add_to_cgroup_hook A lifecycle hook that the forked process will
   *        call after fork and before exec to move itself into the appropriate cgroup.
   * @note The given environment variables will be simply added to the process's
   *          environment in addition to the existing environment variables.
   *          Behaviors of duplicate environment variables will be undefined.
   * @note The subprocess is the child of this process, so it's caller process's duty to
   * handle SIGCHLD signal and reap the zombie children.
   * @note If RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper is set to
   *       true, Raylet will kill any orphan grandchildren processes when the spawned
   * process dies, *even if* `decouple` is set to `true`.
   */
  explicit Process(
      const char *argv[],
      std::error_code &ec,
      bool decouple = false,
      const ProcessEnvironment &env = {},
      bool pipe_to_stdin = false,
      AddProcessToCgroupHook add_to_cgroup_hook = [](const std::string &) {},
      bool new_process_group = false);

  /**
   * @brief Creates a process object from an existing PID.
   * @param[in] pid The PID of the process to create.
   * @return A unique pointer to the process object.
   */
  explicit Process(pid_t pid);

  Process(Process &&other);
  Process &operator=(Process &&other);
  Process(const Process &other) = delete;
  Process &operator=(const Process &other) = delete;

  /**
   * @brief Convenience function to run the given command line and wait for it to finish.
   * @param[in] args The command-line arguments.
   * @param[in] env Additional environment variables.
   * @note The given environment variables will be simply added to the process's
   *          environment in addition to the existing environment variables.
   *          Behaviors of duplicate environment variables will be undefined.
   * @return Any error that occurred.
   */
  static std::error_code Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env = {});

  /**
   * @brief Executes command line operation.
   * @param[in] argv The command line command to execute.
   * @return The output from the command.
   */
  static std::string Exec(const std::string command);

  /**
   * @brief Convenience function to start a process in the background.
   * @param[in] args The command-line arguments.
   * @param[in] decouple True iff the parent will not wait for the child to exit.
   * @param[in] pid_file A file to write the PID of the spawned process in.
   * @param[in] env Additional environment variables.
   * @param[in] new_process_group Whether to create a new process group.
   * @note The given environment variables will be simply added to the process's
   *          environment in addition to the existing environment variables.
   *          Behaviors of duplicate environment variables will be undefined.
   * @return A pair containing the process object and the error code if any error
   * occurred.
   */
  static std::pair<std::unique_ptr<ProcessInterface>, std::error_code> Spawn(
      const std::vector<std::string> &args,
      bool decouple,
      const std::string &pid_file = std::string(),
      const ProcessEnvironment &env = {},
      bool new_process_group = false);

  pid_t GetId() const override;

  bool IsNull() const override;

  bool IsValid() const override;

  void Kill() override;

  bool IsAlive() const override;

  int Wait() const override;

 private:
  pid_t pid_;

  // The pipe's fd to track the child process's lifetime.
  intptr_t fd_;

  /**
   * @brief Spawns a new process with the given arguments and environment,
   *        and tracks it within this Process.
   * @details This function creates two pipes one for the current process
   *          to track the child process's lifetime and one for the child process
   *          to track the parent process's lifetime. The fd of the read end
   *          of the pipe to track child process's lifetime is returned.
   * @param[in] argv The command-line arguments to spawn the process with.
   * @param[out] ec Returns any error that occurred when spawning the process.
   * @param[in] decouple Whether to decouple the parent and child processes.
   * @param[in] env The environment variables to set for the process.
   * @param[in] pipe_to_stdin Whether to pipe the child's stdin to the parent.
   * @param[in] add_to_cgroup A function to add the child to a cgroup.
   * @param[in] new_process_group Whether to create a new process group.
   * @note The given environment variables will be simply added to the process's
   *          environment in addition to the existing environment variables.
   *          Behaviors of duplicate environment variables will be undefined.
   * @return A pair of the process ID and the fd of the read end of the pipe
   *         to track child process's lifetime.
   *         Invariant: The pid and fd returned by this function are either
   *         always both valid or both invalid.
   */
  std::pair<pid_t, intptr_t> Spawnvpe(const char *argv[],
                                      std::error_code &ec,
                                      bool decouple,
                                      const ProcessEnvironment &env,
                                      bool pipe_to_stdin,
                                      AddProcessToCgroupHook add_to_cgroup,
                                      bool new_process_group) const;
};

}  // namespace ray

// We only define operators required by the standard library (==, hash):
// -   Valid process objects must be distinguished by their IDs.
// - Invalid process objects must be distinguished by their addresses.
namespace std {

template <>
struct equal_to<ray::Process> {
  bool operator()(const ray::Process &x, const ray::Process &y) const;
};

template <>
struct hash<ray::Process> {
  size_t operator()(const ray::Process &value) const;
};

}  // namespace std
