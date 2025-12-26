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

#include <cctype>
#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include "ray/util/compat.h"

#ifndef PID_MAX_LIMIT
// This is defined by Linux to be the maximum allowable number of processes
// There's no guarantee for other OSes, but it's useful for testing purposes.
enum { PID_MAX_LIMIT = 1 << 22 };
#endif

namespace ray {

class EnvironmentVariableLess {
 public:
  bool operator()(char a, char b) const {
    // TODO(mehrdadn): This is only used on Windows due to current lack of Unicode
    // support. It should be changed when Process adds Unicode support on Windows.
    return std::less<>()(tolower(a), tolower(b));
  }

  bool operator()(const std::string &a, const std::string &b) const {
    bool result = false;
#ifdef _WIN32
    result = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(), *this);
#else
    result = a < b;
#endif
    return result;
  }
};

using ProcessEnvironment = std::map<std::string, std::string, EnvironmentVariableLess>;

using StartupToken = int64_t;

/// \class ProcessInterface
///
/// Interface for process
class ProcessInterface {
 public:
  virtual ~ProcessInterface() = default;

  /// Returns the process ID.
  /// \return The process ID, or -1 for a null/dummy process.
  virtual pid_t GetId() const = 0;

  /// Returns an opaque pointer or handle to the underlying process object.
  /// Implementation detail, used only for identity testing. Do not dereference.
  virtual const void *Get() const = 0;

  /// Returns true if this is a null process object.
  virtual bool IsNull() const = 0;

  /// Returns true if this process has a valid (non-negative) PID.
  virtual bool IsValid() const = 0;

  /// Forcefully kills the process.
  /// Unsafe for unowned processes.
  virtual void Kill() = 0;

  /// Check whether the process is alive.
  virtual bool IsAlive() const = 0;

  /// Waits for process to terminate.
  /// Not supported for unowned processes.
  /// \return The process's exit code. Returns 0 for a dummy process, -1 for a null one.
  virtual int Wait() const = 0;
};

}  // namespace ray
