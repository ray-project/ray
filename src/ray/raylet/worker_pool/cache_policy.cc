// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// //  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/worker_pool/cache_policy.h"

#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace {}  // namespace

namespace ray {

namespace raylet {

// FutureIdlePoolSizePolicy::FutureIdlePoolSizePolicy(size_t desired_cache_size,
//                                       size_t max_starting_size) :
//                                       desired_cache_size_(desired_cache_size),
//                                       max_starting_size_(max_starting_size) {}

FutureIdlePoolSizePolicy::FutureIdlePoolSizePolicy(
    size_t desired_cache_size,
    size_t max_starting_size,
    const std::function<double()> get_time) {
  // TODO(cade) rename
  desired_cache_size_ = 0;
  max_starting_size_ = max_starting_size;
  get_time_ = get_time;
}

size_t FutureIdlePoolSizePolicy::GetNumIdleProcsToCreate(size_t idle_size,
                                                         size_t running_size,
                                                         size_t starting_size) {
  // Running: means that the process is running
  // Idle: Usually running. Means that the process doesn't have any task.
  // Starting: Process that was started but hasn't finished initialization.
  // Pending exit: Process that is exiting but hasn't confirmed.

  RAY_LOG(DEBUG) << "Idle size: " << idle_size << ", running_size: " << running_size
                 << ", starting_size: " << starting_size;
  auto to_add = desired_cache_size_ - running_size;

  auto to_add_capped =
      (size_t)std::min(max_starting_size_, (size_t)std::max((size_t)0, to_add));
  return to_add_capped;
}

// How to get this in incrementally?
// The worker killing poller operates on a per-worker basis.
// We need to determine which workers to kill...
// So we can either go with a very simple policy, or very simple policy plus policy which
// determines
//      which workers to kill.
std::vector<std::shared_ptr<WorkerInterface>>
FutureIdlePoolSizePolicy::GetIdleProcsToKill(
    size_t alive_size,
    size_t pending_exit_size,
    size_t starting_size,
    const std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
        &idle_of_all_languages,
    std::function<std::unordered_set<std::shared_ptr<WorkerInterface>>(
        std::shared_ptr<WorkerInterface>)> GetFateSharingWorkers,
    std::function<bool(int64_t,
                       const std::unordered_set<std::shared_ptr<WorkerInterface>>
                           &fate_sharers)> CanKillFateSharingWorkers) {
  std::vector<std::shared_ptr<WorkerInterface>> to_kill;
  Populate(to_kill,
           alive_size,
           pending_exit_size,
           idle_of_all_languages,
           GetFateSharingWorkers,
           CanKillFateSharingWorkers);
  return to_kill;
}

// TODO name
// Inputs:
// get_time_
// GetAllRegisteredWorkers (for running_size calc)
// pending_exit_idle_workers_ (assertion, for running_size calc)
// idle_of_all_languages_ (for scanning which workers to remove)
// finished_jobs_ (for scanning which jobs are finished)
// worker_pool_.states_by_lang_.at
// (worker_state.worker_processes.find(worker_startup_token); for some java stuff) (also
// non java stuff) (Make sure all workers in this worker process are idle.)
// worker_pool_.GetWorkersByProcess (multiple java workers per worker)
// worker_pool_.idle_of_all_languages_map_.at(worker) (for idle timeout)
// worker_pool_.pending_exit_idle_workers_ (can't kill these)
// num_workers_soft_limit_ (easy)
//
// Synthesizing:
// now_time
// running_size (requires GetAllRegisteredWorkers + pending_exit_idle_workers_)
// candidates (idle_of_all_languages_)
//
// For each idle worker
//      If the job is done, kill.
//              Pass finished jobs
//      If the worker is not idle for long enough, skip.
//              Check via worker
//      If the worker is already dead, ignore (java can have dead workers and live workers
//      in same proc?)
//              Check via worker
//      If the worker process is pending registration, ignore (java can have other workers
//      pending registration)
//              Have callback that determines if a worker can be killed?
//              std::optional<std::vector<WorkerInterface>> GetFateSharingWorkers() [const
//              WorkerPool&] GetFateSharingWorkers -> std:vector<WorkerInterface>
//              CanKillFateSharingWorkers -> bool
//      If there are other workers in the same process that are recently active, skip.
//              Same as above
//      If there are other workers in the same process that are pending exit, skip.
//              Same as above
//      If killing all workers in the process kill too many workers, skip. (likely broken
//      in current impl).
//              Easy
//
//
void FutureIdlePoolSizePolicy::Populate(
    std::vector<std::shared_ptr<WorkerInterface>> &idle_workers_to_remove,
    size_t alive_size,
    size_t pending_exit_size,
    const std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
        &idle_of_all_languages,
    std::function<std::unordered_set<std::shared_ptr<WorkerInterface>>(
        std::shared_ptr<WorkerInterface>)> GetFateSharingWorkers,
    std::function<bool(int64_t,
                       const std::unordered_set<std::shared_ptr<WorkerInterface>>
                           &fate_sharers)> CanKillFateSharingWorkers) {
  int64_t now = get_time_();

  // Subtract the number of pending exit workers first. This will help us killing more
  // idle workers that it needs to.
  RAY_CHECK(alive_size >= pending_exit_size);
  size_t running_size = alive_size - pending_exit_size;

  // TODO move
  size_t num_workers_soft_limit_ = 64;

  for (const auto &idle_pair : idle_of_all_languages) {
    const auto &idle_worker = idle_pair.first;
    const auto &job_id = idle_worker->GetAssignedJobId();

    RAY_LOG(DEBUG) << " Checking idle worker "
                   << idle_worker->GetAssignedTask().GetTaskSpecification().DebugString()
                   << " worker id " << idle_worker->WorkerId();

    if (running_size <= static_cast<size_t>(num_workers_soft_limit_)) {
      if (!finished_jobs_.contains(job_id)) {
        // Ignore the soft limit for jobs that have already finished, as we
        // should always clean up these workers.
        RAY_LOG(DEBUG) << "Job not finished. Not going to kill worker "
                       << idle_worker->WorkerId();
        continue;
      }
    }

    if (now - idle_pair.second <
        RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
      break;
    }
    if (idle_worker->IsDead()) {
      RAY_LOG(DEBUG) << "idle worker is already dead. Not going to kill worker "
                     << idle_worker->WorkerId();
      // This worker has already been killed.
      // This is possible because a Java worker process may hold multiple workers.
      continue;
    }

    auto fate_sharing_workers = GetFateSharingWorkers(idle_worker);
    bool can_kill_fate_sharing_workers =
        CanKillFateSharingWorkers(now, fate_sharing_workers);

    if (!can_kill_fate_sharing_workers) {
      continue;
    }

    RAY_CHECK(running_size >= fate_sharing_workers.size());
    if (running_size - fate_sharing_workers.size() <
        static_cast<size_t>(num_workers_soft_limit_)) {
      // A Java worker process may contain multiple workers. Killing more workers than
      // we expect may slow the job.
      // Semantics: if the job has not finished, return. (Unclear why?)
      // TODO(cade) add comment here / fix this ??
      if (finished_jobs_.count(job_id) == 0) {
        // Ignore the soft limit for jobs that have already finished, as we
        // should always clean up these workers.
        return;
      }
    }

    for (const auto &worker : fate_sharing_workers) {
      RAY_LOG(DEBUG) << "The worker pool has " << running_size
                     << " registered workers which exceeds the soft limit of "
                     << num_workers_soft_limit_ << ", and worker " << worker->WorkerId()
                     << " with pid " << idle_worker->GetProcess().GetId()
                     << " has been idle for a a while. Kill it.";

      idle_workers_to_remove.push_back(worker);
      running_size--;
    }
  }
}

void FutureIdlePoolSizePolicy::OnStart() {
  if (!RayConfig::instance().enable_worker_prestart()) {
    return;
  }

  // set desired to 64
  desired_cache_size_ = 64;
}

// TODO(cade) for current policy, track whether or not have seen first driver.
void FutureIdlePoolSizePolicy::OnDriverRegistered() {
  if (!RayConfig::instance().prestart_worker_first_driver() ||
      RayConfig::instance().enable_worker_prestart()) {
    return;
  }

  desired_cache_size_ = 64;
}

void FutureIdlePoolSizePolicy::OnJobTermination() {}
void FutureIdlePoolSizePolicy::OnJobFinish(const JobID &job_id) {
  finished_jobs_.insert(job_id);
}
void FutureIdlePoolSizePolicy::OnPrestart() {}

}  // namespace raylet

}  // namespace ray
