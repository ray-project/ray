// Copyright 2026 The Ray Authors.
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

#include "ray/raylet/worker_killing_policy_by_time.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "ray/common/lease/lease.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/util/compat.h"

namespace ray {

namespace raylet {

TimeBasedWorkerKillingPolicy::TimeBasedWorkerKillingPolicy(int64_t threshold_bytes,
                                                           int64_t kill_buffer_bytes)
    : threshold_bytes_(threshold_bytes),
      kill_buffer_bytes_(kill_buffer_bytes),
      idle_worker_killing_memory_threshold_bytes_(
          RayConfig::instance().idle_worker_killing_memory_threshold_bytes()) {}

std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
TimeBasedWorkerKillingPolicy::SelectWorkersToKill(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const ProcessesMemorySnapshot &process_memory_snapshot,
    const SystemMemorySnapshot &system_memory_snapshot) {
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> remaining_alive_targets;
  std::vector<std::string> alive_worker_debug_strings;
  for (const auto &worker_being_killed_or_should_retry : workers_being_killed_) {
    std::shared_ptr<WorkerInterface> worker = worker_being_killed_or_should_retry.first;
    if (worker->GetProcess().IsAlive()) {
      alive_worker_debug_strings.push_back(
          absl::StrFormat("(Worker's Lease ID: %s, Worker PID: %d)",
                          worker->GetGrantedLeaseId().Hex(),
                          worker->GetProcess().GetId()));
      remaining_alive_targets.push_back(worker_being_killed_or_should_retry);
    }
  }

  workers_being_killed_ = remaining_alive_targets;
  if (workers_being_killed_.empty()) {
    workers_being_killed_ =
        Policy(workers, process_memory_snapshot, system_memory_snapshot);
    if (workers_being_killed_.empty()) {
      RAY_LOG_EVERY_MS(WARNING, 5000)
          << "Worker killer did not select any workers to "
             "kill even though memory usage is high. Other Ray processes (e.g. driver, "
             "raylet, dashboard agent, runtime environment agent, GCS server, "
             "API server, etc.) or other non-ray processes may be occupying most of "
             "the memory.";
    }
    return workers_being_killed_;
  }
  RAY_LOG(INFO) << absl::StrFormat(
      "Still waiting for worker eviction to free up memory. Alive workers: [%s]",
      absl::StrJoin(alive_worker_debug_strings, ", "));
  return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
}

std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
TimeBasedWorkerKillingPolicy::Policy(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const ProcessesMemorySnapshot &process_memory_snapshot,
    const SystemMemorySnapshot &system_memory_snapshot) const {
  if (workers.empty()) {
    return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
  }

  std::vector<std::shared_ptr<WorkerInterface>> sorted_workers;
  // Filter workers without lease that are too small to be considered for killing.
  std::copy_if(
      workers.begin(),
      workers.end(),
      std::back_inserter(sorted_workers),
      [threshold = idle_worker_killing_memory_threshold_bytes_,
       &process_memory_snapshot](const std::shared_ptr<WorkerInterface> &worker) -> bool {
        int64_t used_memory = MemoryMonitorUtils::GetProcessUsedMemoryBytes(
            process_memory_snapshot, worker->GetProcess().GetId());
        return !worker->GetGrantedLeaseId().IsNil() || used_memory >= threshold;
      });

  // Sort by:
  // 1. Workers without granted lease larger than idle worker killing memory threshold
  // first
  // 2. Retriable tasks next
  // 3. Most recent last (newest granted lease time)
  std::sort(
      sorted_workers.begin(),
      sorted_workers.end(),
      [](const std::shared_ptr<WorkerInterface> &left,
         const std::shared_ptr<WorkerInterface> &right) -> bool {
        if (left->GetGrantedLeaseId().IsNil() && !right->GetGrantedLeaseId().IsNil()) {
          return true;
        }
        if (right->GetGrantedLeaseId().IsNil() && !left->GetGrantedLeaseId().IsNil()) {
          return false;
        }

        if (left->GetGrantedLease().GetLeaseSpecification().IsRetriable() &&
            !right->GetGrantedLease().GetLeaseSpecification().IsRetriable()) {
          return true;
        }
        if (!left->GetGrantedLease().GetLeaseSpecification().IsRetriable() &&
            right->GetGrantedLease().GetLeaseSpecification().IsRetriable()) {
          return false;
        }

        return left->GetGrantedLeaseTime() > right->GetGrantedLeaseTime();
      });

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill;
  // continue to select workers until the memory to free is reached
  auto sorted_worker_it = sorted_workers.begin();
  int64_t memory_to_free_bytes =
      system_memory_snapshot.used_bytes - threshold_bytes_ + kill_buffer_bytes_;
  int64_t memory_left_to_free = memory_to_free_bytes;

  while (memory_left_to_free > 0 && sorted_worker_it != sorted_workers.end()) {
    std::shared_ptr<WorkerInterface> worker_to_kill = *sorted_worker_it;
    bool should_retry =
        worker_to_kill->GetGrantedLease().GetLeaseSpecification().IsRetriable();
    workers_to_kill.push_back(std::make_pair(worker_to_kill, should_retry));

    pid_t worker_pid = worker_to_kill->GetProcess().GetId();
    const auto worker_pid_entry = process_memory_snapshot.find(worker_pid);
    if (worker_pid_entry != process_memory_snapshot.end()) {
      memory_left_to_free -= worker_pid_entry->second;
    } else {
      RAY_LOG(WARNING) << absl::StrFormat(
          "Attempting to kill worker with PID: %d, but can't account for memory usage of "
          "this "
          "worker to kill. The underlying process may have already been killed or died.",
          worker_pid);
    }
    sorted_worker_it++;
  }

  RAY_LOG(DEBUG) << absl::StrFormat(
      "Needed to free %d bytes. Selected %d workers to kill: %s",
      memory_to_free_bytes,
      workers_to_kill.size(),
      PolicyDebugString(workers_to_kill, process_memory_snapshot));

  return workers_to_kill;
}

std::string TimeBasedWorkerKillingPolicy::PolicyDebugString(
    const std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> &workers,
    const ProcessesMemorySnapshot &process_memory_snapshot) {
  std::stringstream result;
  result << "Workers sorted by time based worker killing policy: ";

  std::vector<std::string> worker_debug_strings;
  for (const auto &[worker, _] : workers) {
    pid_t pid = worker->GetProcess().GetId();
    int64_t used_memory = 0;
    const auto pid_entry = process_memory_snapshot.find(pid);
    if (pid_entry != process_memory_snapshot.end()) {
      used_memory = pid_entry->second;
    } else {
      RAY_LOG_EVERY_MS(INFO, 60000) << absl::StrFormat(
          "Can't find memory usage for PID, reporting zero. PID: %d", pid);
    }

    if (worker_debug_strings.size() >= 10) {
      worker_debug_strings.push_back(absl::StrFormat(
          "  ... (%zu more workers)", workers.size() - worker_debug_strings.size()));
      break;
    }

    bool retriable = worker->GetGrantedLease().GetLeaseSpecification().IsRetriable();
    worker_debug_strings.push_back(absl::StrFormat(
        "(Worker's Lease ID: %s | Granted time: %s | Retriable: %s | Memory used: %d "
        "bytes)",
        worker->GetGrantedLeaseId().Hex(),
        absl::FormatTime(worker->GetGrantedLeaseTime(), absl::UTCTimeZone()),
        retriable ? "yes" : "no",
        used_memory));
  }

  result << absl::StrJoin(worker_debug_strings, ", ");
  return result.str();
}

}  // namespace raylet

}  // namespace ray
