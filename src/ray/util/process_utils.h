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

#include <optional>
#include <system_error>
#include <vector>

#include "ray/util/compat.h"

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
