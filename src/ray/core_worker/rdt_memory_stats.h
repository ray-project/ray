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

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

/// Process-local snapshot of RDT (Ray Direct Transport) object residency.
///
/// The Python RDT store publishes a scalar-only snapshot (object id -> device /
/// byte / copy / error counts) on each add/pop mutation. GetCoreWorkerStats reads
/// the snapshot without touching Python. The map never owns tensors or
/// ObjectRefs; it stores an ObjectID key and scalar counters only.
class RdtMemoryStats {
 public:
  RdtMemoryStats() = default;

  /// Replace the snapshot for `object_id`. Passing `num_copies == 0 &&
  /// num_errors == 0` erases the entry.
  void Set(const ObjectID &object_id,
           const std::vector<std::pair<std::string, int64_t>> &tensor_bytes,
           int64_t num_copies,
           int64_t num_errors);

  /// Append up to `limit` entries (-1 means unlimited) to `stats`, and always
  /// set `rdt_objects_total` independently of `limit`.
  void AddToStats(rpc::CoreWorkerStats *stats, int64_t limit) const;

 private:
  mutable absl::Mutex mutex_;
  absl::flat_hash_map<ObjectID, rpc::RdtObjectMemoryInfo> objects_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace core
}  // namespace ray
