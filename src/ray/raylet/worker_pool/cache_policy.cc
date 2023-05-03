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
  prestart_requests_ = 0;
}

size_t FutureIdlePoolSizePolicy::GetNumIdleProcsToCreate(size_t idle_size,
                                                         size_t running_size,
                                                         size_t starting_size) {
  bool use_new_policy = false;
  if (use_new_policy) {
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
  } else {
    size_t max_cpu = 64;
    size_t num_available_cpus = std::max<size_t>(max_cpu - running_size, 0);  // TODO
    auto num_usable_workers = idle_size + starting_size;
    auto num_desired_workers = std::min<size_t>(prestart_requests_, num_available_cpus);

    size_t num_to_create = 0;
    if (num_desired_workers > num_usable_workers) {
      auto num_missing_workers = num_desired_workers - num_usable_workers;
      num_to_create = num_missing_workers;
    }

    // this->prestart_requests_ -= num_to_create;
    this->prestart_requests_ = 0;
    return num_to_create;

    // auto &state = GetStateForLanguage(task_spec.GetLanguage());
    //// The number of available workers that can be used for this task spec.
    // int num_usable_workers = state.idle.size();
    // for (auto &entry : state.worker_processes) {
    //  num_usable_workers += entry.second.is_pending_registration ? 1 : 0;
    //}
    //// Some existing workers may be holding less than 1 CPU each, so we should
    //// start as many workers as needed to fill up the remaining CPUs.
    // auto desired_usable_workers = std::min<int64_t>(num_available_cpus, backlog_size);
    // if (num_usable_workers < desired_usable_workers) {
    //  // Account for workers that are idle or already starting.
    //  int64_t num_needed = desired_usable_workers - num_usable_workers;
    //  RAY_LOG(DEBUG) << "Prestarting " << num_needed << " workers given task backlog
    //  size "
    //                 << backlog_size << " and available CPUs " << num_available_cpus;
    //  // musing(cade): I should not use the MaybeRefillIdlePool function here as it's a
    //  // scheduling hint (not desired state). We should modify this later, for example
    //  with
    //  // forkserver. This means that Ray's goodness depends on waiting 2s for killing...
    //  PrestartDefaultCpuWorkers(task_spec.GetLanguage(), num_needed);
    //}
  }
}

// How to get this in incrementally?
// The worker killing poller operates on a per-worker basis.
// We need to determine which workers to kill...
// So we can either go with a very simple policy, or very simple policy plus policy which
// determines
//      which workers to kill.
std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>>
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
  std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>> to_kill;
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
    // std::vector<std::shared_ptr<WorkerInterface>> &idle_workers_to_remove,
    std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>> &idle_workers_to_remove,
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
      bool force_kill = finished_jobs_.contains(worker->GetAssignedJobId()) &&
                        RayConfig::instance().kill_idle_workers_of_terminated_job();
      idle_workers_to_remove.push_back(std::make_pair(worker, force_kill));
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

  this->prestart_requests_ += 64;  // num_prestart_python_workers
  desired_cache_size_ = 64;
}

void FutureIdlePoolSizePolicy::OnJobTermination() {}

void FutureIdlePoolSizePolicy::OnJobFinish(const JobID &job_id) {
  finished_jobs_.insert(job_id);
}

void FutureIdlePoolSizePolicy::OnPrestart(const TaskSpecification &task_spec,
                                          int64_t backlog_size,
                                          int64_t num_available_cpus) {
  if ((task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) ||
      task_spec.HasRuntimeEnv() || task_spec.GetLanguage() != ray::Language::PYTHON) {
    return;  // Not handled.
    // TODO(architkulkarni): We'd eventually like to prestart workers with the same
    // runtime env to improve initial startup performance.
  }

  // What do I need?
  // I need to distinguish between soft limit and desired creation.
  // This will change desired creation to nonzero.
  // this->desired_cache_size_ = std::min<int64_t>(num_available_cpus, backlog_size);
  this->prestart_requests_ += backlog_size;

  // If the prestart request is for dynamic options, or actor creation, or runtime env, or
  // not python, then skip. Get number of existing idle workers that can be used for this
  // task spec Get number of desired workers, limited by available CPUs (or backlog size,
  // which ever is smaller) If the number of existing idle workers is smaller than desired
  // workers, then prestart the delta.
  //

  // Number of usable workers is idle size plus starting size.
  // I should modify the get num idle proc to create function to understand the idle size
  // and starting size. then this logic just goes there -- we save the number of prestart
  // backlog,

  // Okay, what if I remove the num_available_cpus here, and pass the
  // cluster_resource_scheduler to the policy?
  // Then this becomes MaybeRefill(create=true), with GetNumIdleProcsToCreate takes in
  // idle size, starting size, available cpus

  // Current behavior:
  // Pool doesn't eagerly grow. Processes are created once the reequest is known.
  // The PrestartWorkers feature pre-starts processes once many tasks are known
  // (backlog>1). This means that the desired pool size grows to 64-in_use. After that,
  // the pool shrinks.. So, current logic should take the spike. No levelling behavior.

  // TODO(cade) this one requires more work.

  // auto &state = GetStateForLanguage(task_spec.GetLanguage());
  //// The number of available workers that can be used for this task spec.
  // int num_usable_workers = state.idle.size();
  // for (auto &entry : state.worker_processes) {
  //  num_usable_workers += entry.second.is_pending_registration ? 1 : 0;
  //}
  //// Some existing workers may be holding less than 1 CPU each, so we should
  //// start as many workers as needed to fill up the remaining CPUs.
  // auto desired_usable_workers = std::min<int64_t>(num_available_cpus, backlog_size);
  // if (num_usable_workers < desired_usable_workers) {
  //  // Account for workers that are idle or already starting.
  //  int64_t num_needed = desired_usable_workers - num_usable_workers;
  //  RAY_LOG(DEBUG) << "Prestarting " << num_needed << " workers given task backlog size
  //  "
  //                 << backlog_size << " and available CPUs " << num_available_cpus;
  //  // musing(cade): I should not use the MaybeRefillIdlePool function here as it's a
  //  // scheduling hint (not desired state). We should modify this later, for example
  //  with
  //  // forkserver. This means that Ray's goodness depends on waiting 2s for killing...
  //  PrestartDefaultCpuWorkers(task_spec.GetLanguage(), num_needed);
  //}
}

}  // namespace raylet

}  // namespace ray
