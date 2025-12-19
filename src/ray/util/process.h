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

#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "ray/util/compat.h"
#include "ray/util/logging.h"

// TODO(#54703): Put this type in a separate target.
using AddProcessToCgroupHook = std::function<void(const std::string &)>;

#ifndef PID_MAX_LIMIT
// This is defined by Linux to be the maximum allowable number of processes
// There's no guarantee for other OSes, but it's useful for testing purposes.
enum { PID_MAX_LIMIT = 1 << 22 };
#endif

namespace ray {

#if !defined(_WIN32)
/// Sets the FD_CLOEXEC flag on a file descriptor.
/// This means when the process is forked, this fd would be closed in the child process
/// side.
///
/// Idempotent.
/// Not thread safe.
/// See https://github.com/ray-project/ray/issues/40813
void SetFdCloseOnExec(int fd);
#endif

class EnvironmentVariableLess {
 public:
  bool operator()(char a, char b) const;

  bool operator()(const std::string &a, const std::string &b) const;
};

typedef std::map<std::string, std::string, EnvironmentVariableLess> ProcessEnvironment;

using StartupToken = int64_t;

class ProcessFD;

class Process {
 protected:
  std::shared_ptr<ProcessFD> p_;

  explicit Process(pid_t pid);

 public:
  ~Process();
  /// Creates a null process object. Two null process objects are assumed equal.
  Process();
  Process(const Process &);
  Process(Process &&);
  Process &operator=(Process other);
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
  /// Convenience function to run the given command line and wait for it to finish.
  static std::error_code Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env = {});
  /// Executes command line operation.
  ///
  /// \param[in] argv The command line command to execute.
  /// \return The output from the command.
  static std::string Exec(const std::string command);
  static Process CreateNewDummy();
  static Process FromPid(pid_t pid);
  pid_t GetId() const;
  /// Returns an opaque pointer or handle to the underlying process object.
  /// Implementation detail, used only for identity testing. Do not dereference.
  const void *Get() const;
  bool IsNull() const;
  bool IsValid() const;
  /// Forcefully kills the process. Unsafe for unowned processes.
  void Kill();
  /// Check whether the process is alive.
  bool IsAlive() const;
  /// Convenience function to start a process in the background.
  /// \param pid_file A file to write the PID of the spawned process in.
  static std::pair<Process, std::error_code> Spawn(
      const std::vector<std::string> &args,
      bool decouple,
      const std::string &pid_file = std::string(),
      const ProcessEnvironment &env = {},
      bool new_process_group = false);
  /// Waits for process to terminate. Not supported for unowned processes.
  /// \return The process's exit code. Returns 0 for a dummy process, -1 for a null one.
  int Wait() const;
};

// Get the Process ID of the parent process. If the parent process exits, the PID
// will be 1 (this simulates POSIX getppid()).
pid_t GetParentPID();

pid_t GetPID();

bool IsParentProcessAlive();

bool IsProcessAlive(pid_t pid);

static constexpr char kProcDirectory[] = "/proc";

// Platform-specific kill for the specified process identifier.
// Currently only supported on Linux. Returns nullopt for other platforms.
std::optional<std::error_code> KillProc(pid_t pid);

// Platform-specific kill for an entire process group. Currently only supported on
// POSIX (non-Windows). Returns nullopt for other platforms.
std::optional<std::error_code> KillProcessGroup(pid_t pgid, int sig);

// Platform-specific utility to find the process IDs of all processes
// that have the specified parent_pid as their parent.
// In other words, find all immediate children of the specified process
// id.
//
// Currently only supported on Linux. Returns nullopt on other platforms.
std::optional<std::vector<pid_t>> GetAllProcsWithPpid(pid_t parent_pid);

/// Terminate the process without cleaning up the resources.
void QuickExit();

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
