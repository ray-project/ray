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

#include <signal.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <vector>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/process.h"

namespace ray {
// Platform-specific implementation of subreaper code.

#ifdef __linux__
namespace {
// Linux implementation.
// If enabled, we set up this process as subreaper. Also in the signal handler we kill
// any unknown children.

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

// Set this process as a subreaper.
// Only works on Linux >= 3.4.
void SetThisProcessAsSubreaper() {
  if (prctl(PR_SET_CHILD_SUBREAPER, 1) == -1) {
    RAY_LOG(FATAL) << "Failed to set this process as subreaper: " << strerror(errno);
  }
}

// Kill all child processes that are not owned by this process.
// It's done by checking the list of child processes and killing the ones that are not
// created via Process::spawnvpe.
//
// TODO: Checking PIDs is not 100% reliable because of PID recycling. If we find issues
// later due to this, we can use pidfd.
void KillUnownedChildren() {
  auto child_procs = GetAllProcsWithPpid(GetPID());

  // Enumerating child procs is not supported on this platform.
  if (!child_procs) {
    RAY_LOG(FATAL) << "Killing leaked procs not supported on this platform. Only "
                      "supports Linux >= 3.4";
    return;
  }
  auto to_kill = OwnedChildrenTracker::instance().listOwnedChildren(*child_procs);
  for (auto pid : to_kill) {
    RAY_LOG(INFO) << "Killing leaked child process " << pid;
    auto error = KillProc(pid);
    if (error) {
      RAY_LOG(WARNING) << "Failed to kill leaked child process " << pid << " with error "
                       << error->message() << ", value = " << error->value();
    }
  }
}

// SIGCHLD handler that reaps dead children and kills unowned children.
// Unowned children processes, or the process not created via Process::spawnvpe, can
// happen when raylet becomes a subreaper, and a grandchild process is created then the
// child process dies. The grandchild process becomes an orphan and is adopted by raylet
// via Linux.
//
// T=0: raylet -> child -> grandchild
// T=1: child receives SIGKILL. grandchild becomes an orphan and is reparented.
// T=2: raylet -> child (zombie); raylet -> grandchild
// T=3: raylet receives SIGCHLD for child
// T=4: raylet reaps child, and finds grandchild is unowned child, killing it.
//
// CAVEAT: We may accidentally kill innocent subprocesses, if the direct child process
// proposefully creates a grandchild process and exits. We need good test and audit to
// make sure this doesn't happen.
void SigchldHandlerKillOrphanSubprocesses(const boost::system::error_code &error,
                                          int signal_number) {
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
    OwnedChildrenTracker::instance().removeOwnedChild(pid);
  }
  KillUnownedChildren();
}
}  // namespace

// If kill_orphan_subprocesses is false, simply reap the zombie children.
// If kill_orphan_subprocesses is true, also sets this process as a subreaper and kill
// all orphaned subprocesses on SIGCHLD handler.
void SetupSigchldHandler(bool kill_orphan_subprocesses,
                         boost::asio::io_context &io_service) {
  if (kill_orphan_subprocesses) {
    SetThisProcessAsSubreaper();
    auto sigchld_signals = std::make_shared<boost::asio::signal_set>(io_service, SIGCHLD);
    RegisterSignalHandlerLoop(sigchld_signals, SigchldHandlerKillOrphanSubprocesses);
    RAY_LOG(INFO) << "Raylet is set as a subreaper and will kill orphan subprocesses.";
  } else {
    signal(SIGCHLD, SIG_IGN);
    RAY_LOG(INFO) << "Raylet will not kill orphan subprocesses.";
  }
}

void OwnedChildrenTracker::addOwnedChild(pid_t pid) {
  absl::MutexLock lock(&m_);
  children_.insert(pid);
}

void OwnedChildrenTracker::removeOwnedChild(pid_t pid) {
  absl::MutexLock lock(&m_);
  children_.erase(pid);
}

std::vector<pid_t> OwnedChildrenTracker::listOwnedChildren(
    const std::vector<pid_t> &pids) {
  absl::MutexLock lock(&m_);
  std::vector<pid_t> result;
  result.reserve(std::min(pids.size(), children_.size()));
  for (pid_t pid : pids) {
    if (children_.count(pid) > 0) {
      result.push_back(pid);
    }
  }
  return result;
}

SigchldMasker::SigchldMasker() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGCHLD);

  // Block SIGCHLD and save the previous signal mask
  if (sigprocmask(SIG_BLOCK, &set, &prev_mask_) < 0) {
    RAY_LOG(WARNING) << "Failed to block SIGCHLD, " << strerror(errno);
  }
}
SigchldMasker::~SigchldMasker() {
  if (sigprocmask(SIG_SETMASK, &prev_mask_, nullptr) < 0) {
    // It's generally bad practice to throw exceptions from destructors
    // so we just print an error message instead
    RAY_LOG(WARNING) << "Failed to restore signal mask, " << strerror(errno);
  }
}

#elif defined(_WIN32)

// Windows implementation.
// Windows does not have signals or subreaper, so we do no-op for all functions.

void SetupSigchldHandler(bool kill_orphan_subprocesses,
                         boost::asio::io_context &io_service) {
  if (kill_orphan_subprocesses) {
    RAY_LOG(WARNING)
        << "`kill_orphan_subprocesses` set to true, but Raylet will not "
           "kill subprocesses because Subreaper is only supported on Linux >= 3.4.";
  }
}
void OwnedChildrenTracker::addOwnedChild(pid_t pid) {}
void OwnedChildrenTracker::removeOwnedChild(pid_t pid) {}
std::vector<pid_t> OwnedChildrenTracker::listOwnedChildren(
    const std::vector<pid_t> &pids) {
  return {};
}

SigchldMasker::SigchldMasker() {}
SigchldMasker::~SigchldMasker() {}

#elif defined(__APPLE__)

// MacOS implementation.
// MacOS has signals, but does not support subreaper, so we ignore SIGCHLD.

void SetupSigchldHandler(bool kill_orphan_subprocesses,
                         boost::asio::io_context &io_service) {
  if (kill_orphan_subprocesses) {
    RAY_LOG(WARNING)
        << "`kill_orphan_subprocesses` set to true, but Raylet will not "
           "kill subprocesses because Subreaper is only supported on Linux >= 3.4.";
  }
  signal(SIGCHLD, SIG_IGN);
}
void OwnedChildrenTracker::addOwnedChild(pid_t pid) {}
void OwnedChildrenTracker::removeOwnedChild(pid_t pid) {}
std::vector<pid_t> OwnedChildrenTracker::listOwnedChildren(
    const std::vector<pid_t> &pids) {
  return {};
}

SigchldMasker::SigchldMasker() {}
SigchldMasker::~SigchldMasker() {}

#else
#error "Only support Linux, Windows and MacOS"
#endif

}  // namespace ray
