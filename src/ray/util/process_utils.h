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

/**
 * @brief Sets the FD_CLOEXEC flag on a file descriptor.
 * @details This means when the process is forked, this fd would be
 *          closed in the child process side.
 *          This should be called on any FD that shouldn't be leaked
 *          to the child process on a fork.
 *
 * @note Idempotent.
 * @note Not thread safe.
 * @param fd The file descriptor to set the FD_CLOEXEC flag on.
 */
void SetFdCloseOnExec(int fd);

/**
 * @brief Get the Process ID of the parent process.
 * @details If the parent process exits, the PID will be 1 (this simulates POSIX
 * getppid()).
 */
pid_t GetParentPID();

pid_t GetPID();

bool IsParentProcessAlive();

bool IsProcessAlive(pid_t pid);

static constexpr char kProcDirectory[] = "/proc";

/**
 * @brief Platform-specific kill for the specified process identifier.
 * @details Currently only supported on Linux. Returns nullopt for other platforms.
 * @param pid The process identifier to kill.
 * @return The error code if the kill failed, nullopt otherwise.
 */
std::optional<std::error_code> KillProc(pid_t pid);

/**
 * @brief Platform-specific kill for an entire process group.
 * @details Currently only supported on POSIX (non-Windows). Returns nullopt for other
 * platforms.
 * @param pgid The process group identifier to kill.
 * @param sig The signal to send to the process group.
 * @return The error code if the kill failed, nullopt otherwise.
 */
std::optional<std::error_code> KillProcessGroup(pid_t pgid, int sig);

/**
 * @brief Platform-specific utility to find the process IDs of all processes
 *        that have the specified parent_pid as their parent.
 * @details Currently only supported on Linux. Returns nullopt on other platforms.
 * @param parent_pid The parent process identifier to find the children of.
 * @return The process IDs of all processes that have the specified parent_pid as their
 * parent.
 */
std::optional<std::vector<pid_t>> GetAllProcsWithPpid(pid_t parent_pid);

/**
 * @brief Terminate the process without cleaning up the resources.
 */
void QuickExit();

}  // namespace ray
