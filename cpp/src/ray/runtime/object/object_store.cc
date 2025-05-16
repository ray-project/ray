// Copyright 2020 The Ray Authors.
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

#include "object_store.h"

#include <memory>
#include <utility>

#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id) {
  PutRaw(data, object_id);
}

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id) {
  PutRaw(data, object_id);
}

std::shared_ptr<msgpack::sbuffer> ObjectStore::Get(const ObjectID &object_id,
                                                   int timeout_ms) {
  return GetRaw(object_id, timeout_ms);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::Get(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  return GetRaw(ids, timeout_ms);
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
ObjectStore::GetAllReferenceCounts() const {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  return core_worker.GetAllReferenceCounts();
}
}  // namespace internal
}  // namespace ray
