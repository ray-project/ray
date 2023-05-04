// Copyright 2017 The Ray Authors.
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

#include <cstddef>
#include <list>
#include <memory>
#include <unordered_set>

#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/raylet/worker.h"

//#include "gtest/gtest.h"

/*

Goal:
    * Implement the current policy behind new interface.
        Prestarting is behind the flag -- so "desired" is zero? and prestart schedules
creation

Worker pool policy
        inputs:
        * on_start
        * on_first_driver
        * on_job_termination
        * on_prestart

        outputs:
        * get_workers_to_create
        * get_workers_to_kill

Current policy:
        inputs:
        * on_start. If prestart is enabled, start N workers. Else do nothing.
        * on_first_driver. If prestart is enabled, start N workers. Else do nothing.
        * on_job_termination. Do nothing?
        * on_prestart. Start up min(max_concurrency, missing_idle_workers).

        outputs:
        * get_workers_to_create. The ones specified by on_prestart.
        * get_workers_to_kill. Any that are >2 seconds old + other conditions.

Future policy:
        inputs:
        * on_start. Set desired number to 64.
        * on_first_driver. Set desired number to 64.
        * on_job_termination. Add terminated job to list of dead jobs.
        * on_prestart. Make sure there are enough idle workers. If not, temporarily boost
the number of idle workers.

        outputs:
        * get_workers_to_create. Anything less than pool size, limited by max_concurrency.
        * get_workers_to_kill. Anything greater than pool size.
*/

namespace ray {

namespace raylet {

class IdlePoolSizePolicyInterface {
 public:
  virtual void OnStart() = 0;
  virtual void OnDriverRegistered() = 0;
  virtual void OnJobTermination() = 0;  // TODO reconcile these two?
  virtual void OnJobFinish(const JobID &job_id) = 0;
  virtual void OnPrestart(const TaskSpecification &task_spec,
                          int64_t backlog_size,
                          int64_t num_available_cpus) = 0;

  virtual size_t GetNumIdleProcsToCreate(size_t idle_size,
                                         size_t running_size,
                                         size_t starting_size) = 0;

  // TODO enum to replace bool
  virtual std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>> GetIdleProcsToKill(
      size_t alive_size,
      size_t pending_exit_size,
      size_t starting_size,
      const std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
          &idle_of_all_languages,
      std::function<std::unordered_set<std::shared_ptr<WorkerInterface>>(
          std::shared_ptr<WorkerInterface>)> GetFateSharingWorkers,
      std::function<bool(int64_t,
                         const std::unordered_set<std::shared_ptr<WorkerInterface>>
                             &fate_sharers)> CanKillFateSharingWorkers) = 0;
};

class FutureIdlePoolSizePolicy : public IdlePoolSizePolicyInterface {
 private:
  size_t desired_cache_size_;
  size_t max_starting_size_;
  size_t prestart_requests_;
  absl::flat_hash_set<JobID> finished_jobs_;
  //std::list<std::pair

  // TODO make these const
  std::function<double()> get_time_;

  void Populate(
      // std::vector<std::shared_ptr<WorkerInterface>> &idle_workers_to_remove,
      std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>>
          &idle_workers_to_remove,
      size_t alive_size,
      size_t pending_exit_size,
      const std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
          &idle_of_all_languages,
      std::function<std::unordered_set<std::shared_ptr<WorkerInterface>>(
          std::shared_ptr<WorkerInterface>)> GetFateSharingWorkers,
      std::function<bool(int64_t,
                         const std::unordered_set<std::shared_ptr<WorkerInterface>>
                             &fate_sharers)> CanKillFateSharingWorkers);

 public:
  FutureIdlePoolSizePolicy(size_t desired_cache_size,
                           size_t max_starting_size,
                           const std::function<double()> get_time_);

  size_t GetNumIdleProcsToCreate(size_t idle_size,
                                 size_t running_size,
                                 size_t starting_size);

  std::list<std::pair<std::shared_ptr<WorkerInterface>, bool>> GetIdleProcsToKill(
      size_t alive_size,
      size_t pending_exit_size,
      size_t starting_size,
      const std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
          &idle_of_all_languages,
      std::function<std::unordered_set<std::shared_ptr<WorkerInterface>>(
          std::shared_ptr<WorkerInterface>)> GetFateSharingWorkers,
      std::function<bool(int64_t,
                         const std::unordered_set<std::shared_ptr<WorkerInterface>>
                             &fate_sharers)> CanKillFateSharingWorkers);
  // Set desired to 64
  void OnStart();

  // Set desired to 64
  void OnDriverRegistered();

  // Add terminated job to list of dead jobs.
  void OnJobTermination();
  void OnJobFinish(const JobID &job_id);

  // Make sure there are enough idle workers. If not, temporarily boost the number of idle
  // workers. (Maybe not temporarily?)
  void OnPrestart(const TaskSpecification &task_spec,
                  int64_t backlog_size,
                  int64_t num_available_cpus);
};

// class WorkerPool : public WorkerPoolInterface {
// public:
//};

}  // namespace raylet

}  // namespace ray
