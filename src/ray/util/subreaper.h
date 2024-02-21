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

#include <signal.h>
#include <sys/types.h>

#include <boost/asio.hpp>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/macros.h"

namespace ray {

// Utility class to enable subreaper functionality in Linux.
//
// In Ray, raylet creates core_worker processes, which may create grandchild processes.
// If core_worker exits normally it tries to kill all its children recursively. However
// if core_worker is killed by a signal, its children leak and still occupy resources.
//
// This class allows raylet to act as a subreaper, so that it can take over the
// responsibility of killing the grandchild processes when core_worker is killed by a
// signal.
//
// How it works: When raylet starts, it calls prctl(PR_SET_CHILD_SUBREAPER, 1) to enable
// subreaper functionality. This means that when a child process of raylet exits, 2 things
// happen:
// 1. the orphaned grandchildren gets reparented to raylet (ppid=raylet's pid)
// 2. raylet gets a SIGCHLD signal.
//
// When raylet receives a SIGCHLD signal, it lists all its children and kills any unknown
// children. This requires raylet to keep track of its *known* children in
// `Process::spawnvpe`.
//
// Subreaper functionality is enabled by setting `kill_child_processes_on_worker_exit` Ray
// config. It is only available in Linux 3.4 and later - if the platform does not support
// subreaper, this class is a no-op (with a warning).

// Sets up sigchld handler for this process.
//
// On Windows: do nothing.
// On Linux: True -> subreaper, False -> ignore sigchld.
// On MacOS: ignore sigchld.
void SetupSigchldHandler(bool kill_orphan_subprocesses,
                         boost::asio::io_context &io_service);

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

// RAII class to mask SIGCHLD in ctor and unmask (back to prev mask) in dtor.
// If masks failed, it logs a warning and carry on.
// Only works in Linux.
// In non-Linux platforms: do nothing.
class SigchldMasker {
 public:
  SigchldMasker();
  ~SigchldMasker();
  RAY_DISALLOW_COPY_AND_ASSIGN(SigchldMasker);

 private:
#ifdef __linux__
  sigset_t prev_mask_;
#endif
};

}  // namespace ray
