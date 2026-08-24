// Copyright 2021 The Ray Authors.
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

#include <functional>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_kv_manager.h"

namespace ray {
namespace gcs {

/// GCSFunctionManager manages the lifecycle of job-associated function and actor
/// metadata.
///
/// This class tracks job reference counts to determine when a job is truly finished
/// (i.e., job has exited AND all detached actors from the job are dead), and performs
/// cleanup of exported functions, actor classes, and worker setup hooks when jobs
/// complete.
///
/// Key responsibilities:
///   - Job reference counting for accurate job completion detection
///   - Cleanup of function/actor metadata from KV store when jobs finish
///   - Handling network retry scenarios for distributed job management
class GCSFunctionManager {
 public:
  explicit GCSFunctionManager(InternalKVInterface &kv,
                              instrumented_io_context &io_context)
      : kv_(kv), io_context_(io_context) {}

  /// Add an operational reference that requires this job's metadata to remain available.
  void AddJobReference(const JobID &job_id) { job_counter_[job_id]++; }

  /// Return whether the job still has any operational references.
  bool HasJobReference(const JobID &job_id) const {
    return job_counter_.contains(job_id);
  }

  /// Set the callback invoked when a job loses its final operational reference.
  void SetJobReferenceReleasedCallback(
      std::function<void(const JobID &)> job_reference_released_callback) {
    job_reference_released_callback_ = std::move(job_reference_released_callback);
  }

  /// Remove one operational reference, cleaning up job metadata at reference count zero.
  void RemoveJobReference(const JobID &job_id) {
    auto iter = job_counter_.find(job_id);
    if (iter == job_counter_.end()) {
      // Job already removed - this is OK for duplicate calls from network retries
      return;
    }

    --iter->second;
    if (iter->second == 0) {
      job_counter_.erase(job_id);
      RemoveExportedFunctions(job_id);
      if (job_reference_released_callback_) {
        job_reference_released_callback_(job_id);
      }
    }
  }

 private:
  void RemoveExportedFunctions(const JobID &job_id) {
    auto job_id_hex = job_id.Hex();
    kv_.Del(
        "fun", "RemoteFunction:" + job_id_hex + ":", true, {[](auto) {}, io_context_});
    kv_.Del("fun", "ActorClass:" + job_id_hex + ":", true, {[](auto) {}, io_context_});
    kv_.Del("fun",
            absl::StrCat(kWorkerSetupHookKeyName, ":", job_id_hex, ":"),
            true,
            {[](auto) {}, io_context_});
  }

  InternalKVInterface &kv_;              // KV store interface for function/actor cleanup
  instrumented_io_context &io_context_;  // IO context for async operations

  /// Reference count per job. A job is considered finished when:
  /// 1. The job/driver has exited, AND 2. All detached actors from the job are dead.
  /// When count reaches zero, function/actor metadata cleanup is triggered.
  absl::flat_hash_map<JobID, size_t> job_counter_;

  /// Invoked when a finished job no longer has an operational reference. This allows
  /// observability-only metadata to become eligible for retention eviction.
  std::function<void(const JobID &)> job_reference_released_callback_;
};

}  // namespace gcs
}  // namespace ray
