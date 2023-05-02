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

//#include "gtest/gtest.h"

/*

Goal:
    remove decision making on how many processes to create from worker pool.
    remove decision making on which processes to kill from the worker pool.

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
	* on_prestart. Make sure there are enough idle workers. If not, temporarily boost the number of idle workers.

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
  virtual void OnJobTermination() = 0;
  virtual void OnPrestart() = 0;

  virtual size_t GetNumIdleProcsToCreate(size_t idle_size,
                                               size_t running_size,
                                               size_t starting_size) = 0;

  virtual size_t GetNumIdleProcsToKill(size_t idle_size,
                                             size_t running_size,
                                             size_t starting_size) = 0;
};

class FutureIdlePoolSizePolicy : public IdlePoolSizePolicyInterface {
 private:
  size_t desired_cache_size_;
  size_t max_starting_size_;

 public:
  FutureIdlePoolSizePolicy(size_t desired_cache_size,
                     size_t max_starting_size);

  size_t GetNumIdleProcsToCreate(size_t idle_size,
                                       size_t running_size,
                                       size_t starting_size);

  size_t GetNumIdleProcsToKill(size_t idle_size,
                                     size_t running_size,
                                     size_t starting_size);
  // Set desired to 64
  void OnStart();

  // Set desired to 64
  void OnDriverRegistered();

  // Add terminated job to list of dead jobs.
  void OnJobTermination();

  // Make sure there are enough idle workers. If not, temporarily boost the number of idle workers.
  // (Maybe not temporarily?)
  void OnPrestart();
};

// class WorkerPool : public WorkerPoolInterface {
// public:
//};

}  // namespace raylet

}  // namespace ray
