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

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/constants.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

/// GcsFunctionManager is a class to manage exported functions in runtime.
/// Right now it only hanldes resource cleanup when it's not needed any more.
/// But for the long term, we should put all function/actor management into
/// this class, includes:
///    - function/actor exporting
///    - function/actor importing
///    - function/actor code life cycle management.
class GCSFunctionManager {
 public:
  explicit GCSFunctionManager(InternalKVInterface &kv,
                              instrumented_io_context &io_context)
      : kv_(kv), io_context_(io_context) {}

  void AddJobReference(const JobID &job_id) { job_counter_[job_id]++; }

  void RemoveJobReference(const JobID &job_id) {
    auto iter = job_counter_.find(job_id);
    RAY_CHECK(iter != job_counter_.end()) << "No such job: " << job_id;
    --iter->second;
    if (iter->second == 0) {
      job_counter_.erase(job_id);
      RemoveExportedFunctions(job_id);
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

  // Handler for internal KV
  InternalKVInterface &kv_;
  instrumented_io_context &io_context_;
  // Counter to check whether the job has finished or not.
  // A job is defined to be in finished status if
  //   1. the job has exited
  //   2. no detached actor from this job is alive
  // Ideally this counting logic should belong to gcs GC manager, but
  // right now, only function manager is using this, it should be ok
  // to just put it here.
  absl::flat_hash_map<JobID, size_t> job_counter_;
};

}  // namespace gcs
}  // namespace ray
