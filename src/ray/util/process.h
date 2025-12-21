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

#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "ray/util/logging.h"
#include "ray/util/process_interface.h"

// TODO(#54703): Put this type in a separate target.
using AddProcessToCgroupHook = std::function<void(const std::string &)>;

namespace ray {

class ProcessFD;

class Process : public ProcessInterface {
 public:
  ~Process() override;

  /// Creates a null process object. Two null process objects are assumed equal.
  Process();

  /// Creates a new process.
  /// \param[in] argv The command-line of the process to spawn (terminated with NULL).
  /// \param[in] io_service Boost.Asio I/O service (optional).
  /// \param[in] ec Returns any error that occurred when spawning the process.
  /// \param[in] decouple True iff the parent will not wait for the child to exit.
  /// \param[in] env Additional environment variables to be set on this process besides
  /// the environment variables of the parent process.
  /// \param[in] pipe_to_stdin If true, it creates a pipe and redirect to child process'
  /// stdin. It is used for health checking from a child process.
  /// Child process can read stdin to detect when the current process dies.
  /// \param add_to_cgroup_hook A lifecycle hook that the forked process will
  /// call after fork and before exec to move itself into the appropriate cgroup.
  //
  // The subprocess is child of this process, so it's caller process's duty to handle
  // SIGCHLD signal and reap the zombie children.
  //
  // Note: if RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper is set to
  // true, Raylet will kill any orphan grandchildren processes when the spawned process
  // dies, *even if* `decouple` is set to `true`.
  explicit Process(
      const char *argv[],
      void *io_service,
      std::error_code &ec,
      bool decouple = false,
      const ProcessEnvironment &env = {},
      bool pipe_to_stdin = false,
      AddProcessToCgroupHook add_to_cgroup_hook = [](const std::string &) {},
      bool new_process_group = false);

  explicit Process(pid_t pid);

  Process(const Process &) = default;
  Process(Process &&) = default;
  Process &operator=(const Process &) = default;
  Process &operator=(Process &&) = default;

 protected:
  std::shared_ptr<ProcessFD> p_;

 public:
  /// Executes command line operation.
  ///
  /// \param[in] argv The command line command to execute.
  /// \return The output from the command.
  static std::string Exec(const std::string command);

  pid_t GetId() const override;

  /// Returns an opaque pointer or handle to the underlying process object.
  /// Implementation detail, used only for identity testing. Do not dereference.
  const void *Get() const override;

  bool IsNull() const override;

  bool IsValid() const override;

  /// Forcefully kills the process. Unsafe for unowned processes.
  void Kill() override;

  /// Check whether the process is alive.
  bool IsAlive() const override;

  /// Waits for process to terminate. Not supported for unowned processes.
  /// \return The process's exit code. Returns 0 for a dummy process, -1 for a null one.
  int Wait() const override;
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
