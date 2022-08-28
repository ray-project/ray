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

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#ifndef PID_MAX_LIMIT
// This is defined by Linux to be the maximum allowable number of processes
// There's no guarantee for other OSes, but it's useful for testing purposes.
enum { PID_MAX_LIMIT = 1 << 22 };
#endif

namespace ray {

class EnvironmentVariableLess {
 public:
  bool operator()(char a, char b) const;

  bool operator()(const std::string &a, const std::string &b) const;
};

typedef std::map<std::string, std::string, EnvironmentVariableLess> ProcessEnvironment;

#ifdef _WIN32
typedef int pid_t;
#endif

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
  explicit Process(const char *argv[],
                   void *io_service,
                   std::error_code &ec,
                   bool decouple = false,
                   const ProcessEnvironment &env = {});
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
      const ProcessEnvironment &env = {});
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
