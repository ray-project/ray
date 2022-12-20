// Copyright 2022 The Ray Authors.
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

#include <gtest/gtest_prod.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/memory_monitor.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

/// Provides the policy on which worker to prioritize killing.
class WorkerKillingPolicy {
 public:
  /// Selects a worker to be killed.
  ///
  /// \param workers the list of candidate workers.
  /// \param system_memory snapshot of memory usage.
  ///
  /// \return the worker to kill, or nullptr if the worker list is empty.
  virtual const std::shared_ptr<WorkerInterface> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const = 0;

  virtual ~WorkerKillingPolicy() {}

 protected:
  /// Returns debug string of the workers.
  ///
  /// \param workers The workers to be printed.
  /// \param num_workers The number of workers to print starting from the beginning of the
  /// worker list.
  /// \param system_memory snapshot of memory usage.
  ///
  /// \return the debug string.
  static std::string WorkersDebugString(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      int32_t num_workers,
      const MemorySnapshot &system_memory);
};

/// Prefers killing retriable workers over non-retriable ones, in LIFO order.
class RetriableLIFOWorkerKillingPolicy : public WorkerKillingPolicy {
 public:
  RetriableLIFOWorkerKillingPolicy();
  const std::shared_ptr<WorkerInterface> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const;
};

}  // namespace raylet

}  // namespace ray
