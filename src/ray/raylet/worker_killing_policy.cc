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

#include "ray/raylet/worker_killing_policy.h"

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

std::string WorkerKillingPolicy::WorkersDebugString(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    int32_t num_workers,
    const MemorySnapshot &system_memory) {
  std::stringstream result;
  int64_t index = 1;
  for (auto &worker : workers) {
    auto pid = worker->GetProcess().GetId();
    int64_t used_memory = 0;
    const auto pid_entry = system_memory.process_used_bytes.find(pid);
    if (pid_entry != system_memory.process_used_bytes.end()) {
      used_memory = pid_entry->second;
    } else {
      RAY_LOG_EVERY_MS(INFO, 60000)
          << "Can't find memory usage for PID, reporting zero. PID: " << pid;
    }
    result << "Worker " << index << ": lease granted time "
           << absl::FormatTime(worker->GetGrantedLeaseTime(), absl::UTCTimeZone())
           << " worker id " << worker->WorkerId() << " memory used " << used_memory
           << " lease spec "
           << worker->GetGrantedLease().GetLeaseSpecification().DebugString() << "\n";

    index += 1;
    if (index > num_workers) {
      break;
    }
  }
  return result.str();
}

}  // namespace raylet

}  // namespace ray
