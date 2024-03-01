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

#pragma once

#ifdef _WIN32
typedef int pid_t;
#else
#include <signal.h>
#include <sys/types.h>
#endif

#include <boost/asio.hpp>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/macros.h"

namespace ray {

#ifdef __linux__
// Utility functions to enable subreaper functionality in Linux.
//
// In Ray, raylet creates core_worker processes, which may create grandchild processes.
// If core_worker exits normally it tries to kill all its children recursively. However
// if core_worker is killed by a signal, its children leak and still occupy resources.
//
// This module allows raylet to act as a subreaper, so that it can take over the
// responsibility of killing the grandchild processes when core_worker is killed by a
// signal.

// Set this process as a subreaper. Should be called by raylet and core_worker.
// Only works on Linux >= 3.4.
// Returns False if the platform does not support subreaper, or the operation fails.
//
// An orphaned process, that is, a process whose parent has exited, gets reparented to
// the nearest ancestor that is a subreaper. We use this to kill orphaned grandchildren
// by this scheme:
//
// raylet (subreaper, killing unknown children)
//  -> core_worker (subreaper, don't kill children until exiting)
//  -> user process
//  -> grandchild process
//
// If core_worker is alive and user process exited, grandchild is reparented to
// core_worker. In this case core_worker will do nothing, because the grandchild may be
// a daemon process.
//
// If core_worker crashed, user process is reparented to raylet. Raylet kills all unknown
// children, so user process is killed, then grandchild is reparented to raylet. Raylet
// kills the grandchild.
bool SetThisProcessAsSubreaper();

// Sets a SIGCHLD handler to remove dead children from KnownChildrenTracker.
// Only works for Linux. No-op on MacOS or Windows.
void SetupSigchldHandlerRemoveKnownChildren(boost::asio::io_context &io_service);

// Kill all unknown children. Lists all processes with pid = this process, and filters
// out the known children, that is, processes created via `Process::spawnvpe`.
//
// Should be called by raylet only.
//
// Note: this function is one-time. Raylet should call it periodically.
void KillUnknownChildren();

#endif

// Ignores SIGCHLD to let the OS auto reap them.
// Available to all platforms.
// Only works for Linux and MacOS. No-op on Windows.
void SetSigchldIgnore();

// Thread-safe tracker for owned children.
// Only works in Linux.
// In non-Linux platforms: do nothing.
class KnownChildrenTracker {
 public:
  static KnownChildrenTracker &instance() {
    static KnownChildrenTracker instance;
    return instance;
  }

  // If the child is already owned, this is a no-op.
  void addKnownChild(pid_t pid);
  // If the child is not owned, this is a no-op.
  void removeKnownChild(pid_t pid);
  // Returns the subset of `pids` that are NOT in the known `children_`.
  // Thread safe.
  std::vector<pid_t> listUnknownChildren(const std::vector<pid_t> &pids);

 private:
  KnownChildrenTracker() = default;
  ~KnownChildrenTracker() = default;
  RAY_DISALLOW_COPY_AND_ASSIGN(KnownChildrenTracker);
#ifdef __linux__
  absl::Mutex m_;
  absl::flat_hash_set<pid_t> children_ ABSL_GUARDED_BY(m_);
#endif
};

}  // namespace ray
