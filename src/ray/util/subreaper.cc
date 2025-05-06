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

#include <algorithm>
#include <memory>
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

namespace {

using SignalHandlerFn = void (*)(const boost::system::error_code &, int);
// Register a signal handler for the given signal.
// The handler will be called with the signal_set and the error code.
// After the handler is called, the signal will be re-registered.
// The callback keeps a reference of the shared ptr to make sure it's not destroyed.
void RegisterSignalHandlerLoop(std::shared_ptr<boost::asio::signal_set> signals,
                               SignalHandlerFn handler) {
  signals->async_wait(
      [signals, handler](const boost::system::error_code &error, int signal_number) {
        handler(error, signal_number);
        RegisterSignalHandlerLoop(signals, handler);
      });
}

void SigchldHandlerReapZombieAndRemoveKnownChildren(
    const boost::system::error_code &error, int signal_number) {
  // The only error is "signal set cancelled" as boost::asio::error::operation_aborted but
  // we won't ever cancel it.
  // https://www.boost.org/doc/libs/1_84_0/doc/html/boost_asio/reference/basic_signal_set/async_wait.html
  // It does not hurt to waitpid anyway.
  if (error) {
    RAY_LOG(WARNING) << "Error in SIGCHLD handler: " << error.message();
  }
  int status;
  pid_t pid;
  // Reaps any children that have exited. WNOHANG makes waitpid non-blocking and returns
  // 0 if there's no zombie children.
  while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    if (WIFEXITED(status)) {
      RAY_LOG(INFO) << "Child process " << pid << " exited with status "
                    << WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      RAY_LOG(INFO) << "Child process " << pid << " exited from signal "
                    << WTERMSIG(status);
    }
    KnownChildrenTracker::instance().RemoveKnownChild(pid);
  }
}

}  // namespace

// Set this process as a subreaper.
// Only works on Linux >= 3.4.
bool SetThisProcessAsSubreaper() {
  if (prctl(PR_SET_CHILD_SUBREAPER, 1) == -1) {
    RAY_LOG(WARNING) << "Failed to set this process as subreaper: " << strerror(errno);
    return false;
  }
  return true;
}

void SetSigchldIgnore() { signal(SIGCHLD, SIG_IGN); }

void SetupSigchldHandlerRemoveKnownChildren(boost::asio::io_context &io_service) {
  auto sigchld_signals = std::make_shared<boost::asio::signal_set>(io_service, SIGCHLD);
  RegisterSignalHandlerLoop(sigchld_signals,
                            SigchldHandlerReapZombieAndRemoveKnownChildren);
  RAY_LOG(INFO)
      << "Raylet is set to reap zombie children and remove known children pids.";
}

// Kill all child processes that are not owned by this process.
// It's done by checking the list of child processes and killing the ones that are not
// created via Process::spawnvpe.
//
// TODO(core): Checking PIDs is not 100% reliable because of PID recycling. If we find
// issues later due to this, we can use pidfd.
void KillUnknownChildren() {
  auto to_kill =
      KnownChildrenTracker::instance().ListUnknownChildren([]() -> std::vector<pid_t> {
        auto child_procs = GetAllProcsWithPpid(GetPID());
        if (!child_procs) {
          RAY_LOG(FATAL) << "Killing leaked procs not supported on this platform. Only "
                            "supports Linux >= 3.4";
          return {};
        }
        return *child_procs;
      });
  for (auto pid : to_kill) {
    RAY_LOG(INFO) << "Killing leaked child process " << pid;
    auto error = KillProc(pid);
    if (error && (*error)) {
      RAY_LOG(WARNING) << "Failed to kill leaked child process " << pid << " with error "
                       << error->message() << ", value = " << error->value();
    }
  }
}

void KnownChildrenTracker::AddKnownChild(std::function<pid_t()> create_child_fn) {
  if (!enabled_) {
    create_child_fn();
    return;
  }
  absl::MutexLock lock(&m_);
  pid_t pid = create_child_fn();
  children_.insert(pid);
}

void KnownChildrenTracker::RemoveKnownChild(pid_t pid) {
  if (!enabled_) {
    return;
  }
  absl::MutexLock lock(&m_);
  children_.erase(pid);
}

std::vector<pid_t> KnownChildrenTracker::ListUnknownChildren(
    std::function<std::vector<pid_t>()> list_pids_fn) {
  if (!enabled_) {
    return list_pids_fn();
  }
  absl::MutexLock lock(&m_);
  std::vector<pid_t> pids = list_pids_fn();
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
// Windows does not have signals or subreaper, so we do no-op.

void SetSigchldIgnore() {}

#elif defined(__APPLE__)

// MacOS implementation.
// MacOS has signals, but does not support subreaper, so we ignore SIGCHLD.

void SetSigchldIgnore() { signal(SIGCHLD, SIG_IGN); }

#else
#error "Only support Linux, Windows and MacOS"
#endif

}  // namespace ray
