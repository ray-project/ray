// Copyright 2024 The Ray Authors.
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

#include "ray/util/subreaper.h"

#include <string.h>

#include <vector>

#ifdef __linux__
#include <sys/prctl.h>
#include <sys/wait.h>
#endif

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/process.h"

namespace ray {
// Platform-specific implementation of subreaper code.

#ifdef __linux__
// Linux implementation.

// Set this process as a subreaper.
// Only works on Linux >= 3.4.
bool SetThisProcessAsSubreaper() {
  if (prctl(PR_SET_CHILD_SUBREAPER, 1) == -1) {
    RAY_LOG(WARNING) << "Failed to set this process as subreaper: " << strerror(errno);
    return false;
  }
  return true;
}

// Kill all child processes that are not owned by this process.
// It's done by checking the list of child processes and killing the ones that are not
// created via Process::spawnvpe.
//
// TODO: Checking PIDs is not 100% reliable because of PID recycling. If we find issues
// later due to this, we can use pidfd.
void KillUnknownChildren() {
  auto child_procs = GetAllProcsWithPpid(GetPID());

  // Enumerating child procs is not supported on this platform.
  if (!child_procs) {
    RAY_LOG(FATAL) << "Killing leaked procs not supported on this platform. Only "
                      "supports Linux >= 3.4";
    return;
  }
  auto to_kill = KnownChildrenTracker::instance().listUnknownChildren(*child_procs);
  for (auto pid : to_kill) {
    RAY_LOG(INFO) << "Killing leaked child process " << pid;
    auto error = KillProc(pid);
    if (error && (*error)) {
      RAY_LOG(WARNING) << "Failed to kill leaked child process " << pid << " with error "
                       << error->message() << ", value = " << error->value();
    }
  }
}

void KnownChildrenTracker::addKnownChild(pid_t pid) {
  absl::MutexLock lock(&m_);
  children_.insert(pid);
}

void KnownChildrenTracker::removeKnownChild(pid_t pid) {
  absl::MutexLock lock(&m_);
  children_.erase(pid);
}

std::vector<pid_t> KnownChildrenTracker::listUnknownChildren(
    const std::vector<pid_t> &pids) {
  absl::MutexLock lock(&m_);
  std::vector<pid_t> result;
  result.reserve(std::min(pids.size(), children_.size()));
  for (pid_t pid : pids) {
    if (children_.count(pid) == 0) {
      result.push_back(pid);
    }
  }
  return result;
}

#elif defined(_WIN32)

// Windows implementation.
// Windows does not have signals or subreaper, so we do no-op for all functions.

void KnownChildrenTracker::addKnownChild(pid_t pid) {}
void KnownChildrenTracker::removeKnownChild(pid_t pid) {}
std::vector<pid_t> KnownChildrenTracker::listUnknownChildren(
    const std::vector<pid_t> &pids) {
  return {};
}

#elif defined(__APPLE__)

// MacOS implementation.
// MacOS has signals, but does not support subreaper, so we ignore SIGCHLD.
void KnownChildrenTracker::addKnownChild(pid_t pid) {}
void KnownChildrenTracker::removeKnownChild(pid_t pid) {}
std::vector<pid_t> KnownChildrenTracker::listUnknownChildren(
    const std::vector<pid_t> &pids) {
  return {};
}

#else
#error "Only support Linux, Windows and MacOS"
#endif

}  // namespace ray
