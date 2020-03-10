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

#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <functional>
#include <memory>
#include <system_error>
#include <utility>

#ifndef PID_MAX_LIMIT
// This is defined by Linux to be the maximum allowable number of processes
// There's no guarantee for other OSes, but it's useful for testing purposes.
enum { PID_MAX_LIMIT = 1 << 22 };
#endif

namespace ray {

#ifdef _WIN32
typedef int pid_t;
#endif

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
  explicit Process(const char *argv[], void *io_service, std::error_code &ec);
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
  /// Waits for process to terminate. Not supported for unowned processes.
  /// \return The process's exit code. Returns 0 for a dummy process, -1 for a null one.
  int Wait() const;
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

#endif
