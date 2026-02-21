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

#ifdef __linux__
#include <signal.h>
#include <sys/types.h>

#include <boost/asio.hpp>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/macros.h"
#endif

namespace ray {

#ifdef __linux__
// Utility functions to enable subreaper functionality in Linux.
//
// In Ray, raylet creates core_worker processes, which may create grandchild processes.
// If core_worker exits normally it tries to kill all its children recursively. However
// if core_worker is crashed unexpectedly, its children leak and still occupy resources.
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
void SetupSigchldHandlerRemoveKnownChildren(boost::asio::io_context &io_service);

// Kill all unknown children. Lists all processes with pid = this process, and filters
// out the known children, that is, processes created via `Process::spawnvpe`.
//
// Should be called by raylet only.
//
// Note: this function is one-time. Raylet should call it periodically.
void KillUnknownChildren();

// Thread-safe tracker for owned children. Provides transactional interfaces for creating
// children and listing children.
// To avoid useless tracking, it should be enabled by calling `Enable` before use. If not
// enabled, it's not tracking anything and is a no-op for each method.
class KnownChildrenTracker {
 public:
  static KnownChildrenTracker &instance() {
    static KnownChildrenTracker instance;
    return instance;
  }

  // Used in initialization to enable the tracker. NOT thread safe.
  void Enable() { enabled_ = true; }

  // Caller may create a child process within `create_child_fn`.
  // if enabled_, adds the child pid to the known `children_`.
  // if !enabled_, simply calls `create_child_fn` and does nothing.
  void AddKnownChild(std::function<pid_t()> create_child_fn);

  // If the child is not owned, this is a no-op.
  // If enabled_, removes the child pid from the known `children_`.
  // If !enabled_, does nothing.
  void RemoveKnownChild(pid_t pid);

  // Caller may list all processes within `list_all_fn`.
  // Returns the subset of fn-returned pids that are NOT in the known `children_`.
  // Thread safe.
  // If enabled_, in a transactional manner, lists the children pids and returns the
  // pids that are not in the known `children_`.
  // If !enabled_, simply calls `list_all_fn` and returns all of the results.
  std::vector<pid_t> ListUnknownChildren(
      std::function<std::vector<pid_t>()> list_pids_fn);

  KnownChildrenTracker(const KnownChildrenTracker &) = delete;
  KnownChildrenTracker &operator=(const KnownChildrenTracker &) = delete;

  ~KnownChildrenTracker() = default;

 private:
  KnownChildrenTracker() = default;

  bool enabled_ = false;
  absl::Mutex m_;
  absl::flat_hash_set<pid_t> children_ ABSL_GUARDED_BY(m_);
};

#endif

// Ignores SIGCHLD to let the OS auto reap them.
// Available to all platforms.
// Only works for Linux and MacOS. No-op on Windows.
void SetSigchldIgnore();

}  // namespace ray
