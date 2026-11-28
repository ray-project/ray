// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/rdt_memory_stats.h"

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

void RdtMemoryStats::Set(const ObjectID &object_id,
                         const std::vector<std::pair<std::string, int64_t>> &tensor_bytes,
                         int64_t num_copies,
                         int64_t num_errors) {
  absl::MutexLock lock(&mutex_);
  if (num_copies == 0 && num_errors == 0) {
    objects_.erase(object_id);
    return;
  }
  rpc::RdtObjectMemoryInfo info;
  info.set_object_id(object_id.Binary());
  for (const auto &[device, bytes] : tensor_bytes) {
    (*info.mutable_tensor_bytes())[device] = bytes;
  }
  info.set_num_copies(num_copies);
  info.set_num_errors(num_errors);
  objects_[object_id] = std::move(info);
}

void RdtMemoryStats::AddToStats(rpc::CoreWorkerStats *stats, int64_t limit) const {
  absl::MutexLock lock(&mutex_);
  stats->set_rdt_objects_total(static_cast<int64_t>(objects_.size()));
  int64_t count = 0;
  for (const auto &entry : objects_) {
    if (limit != -1 && count >= limit) {
      break;
    }
    count += 1;
    stats->add_rdt_objects()->CopyFrom(entry.second);
  }
}

}  // namespace core
}  // namespace ray
