// Copyright 2020-2021 The Ray Authors.
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

#include "local_mode_object_store.h"

#include <ray/api/ray_exception.h>

#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include "../abstract_ray_runtime.h"

namespace ray {
namespace internal {
LocalModeObjectStore::LocalModeObjectStore(LocalModeRayRuntime &local_mode_ray_tuntime)
    : local_mode_ray_tuntime_(local_mode_ray_tuntime) {
  memory_store_ = std::make_unique<CoreWorkerMemoryStore>();
}

void LocalModeObjectStore::PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                                  ObjectID *object_id) {
  *object_id = ObjectID::FromRandom();
  PutRaw(data, *object_id);
}

void LocalModeObjectStore::PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                                  const ObjectID &object_id) {
  auto buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data->data()), data->size(), true);
  auto status = memory_store_->Put(
      ::ray::RayObject(buffer, nullptr, std::vector<rpc::ObjectReference>()), object_id);
  if (!status) {
    throw RayException("Put object error");
  }
}

std::shared_ptr<msgpack::sbuffer> LocalModeObjectStore::GetRaw(const ObjectID &object_id,
                                                               int timeout_ms) {
  std::vector<ObjectID> object_ids;
  object_ids.push_back(object_id);
  auto buffers = GetRaw(object_ids, timeout_ms);
  RAY_CHECK(buffers.size() == 1);
  return buffers[0];
}

std::vector<std::shared_ptr<msgpack::sbuffer>> LocalModeObjectStore::GetRaw(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  std::vector<std::shared_ptr<::ray::RayObject>> results;
  ::ray::Status status = memory_store_->Get(ids,
                                            (int)ids.size(),
                                            timeout_ms,
                                            local_mode_ray_tuntime_.GetWorkerContext(),
                                            false,
                                            &results);
  if (!status.ok()) {
    throw RayException("Get object error: " + status.ToString());
  }
  RAY_CHECK(results.size() == ids.size());
  std::vector<std::shared_ptr<msgpack::sbuffer>> result_sbuffers;
  result_sbuffers.reserve(results.size());
  for (size_t i = 0; i < results.size(); i++) {
    auto data_buffer = results[i]->GetData();
    auto sbuffer = std::make_shared<msgpack::sbuffer>(data_buffer->Size());
    sbuffer->write(reinterpret_cast<const char *>(data_buffer->Data()),
                   data_buffer->Size());
    result_sbuffers.push_back(sbuffer);
  }
  return result_sbuffers;
}

std::vector<bool> LocalModeObjectStore::Wait(const std::vector<ObjectID> &ids,
                                             int num_objects,
                                             int timeout_ms) {
  absl::flat_hash_set<ObjectID> memory_object_ids;
  for (const auto &object_id : ids) {
    memory_object_ids.insert(object_id);
  }
  absl::flat_hash_set<ObjectID> ready;
  ::ray::Status status = memory_store_->Wait(memory_object_ids,
                                             num_objects,
                                             timeout_ms,
                                             local_mode_ray_tuntime_.GetWorkerContext(),
                                             &ready);
  if (!status.ok()) {
    throw RayException("Wait object error: " + status.ToString());
  }
  std::vector<bool> result;
  result.reserve(ids.size());
  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      result.push_back(true);
    } else {
      result.push_back(false);
    }
  }
  return result;
}

void LocalModeObjectStore::AddLocalReference(const std::string &id) { return; }

void LocalModeObjectStore::RemoveLocalReference(const std::string &id) { return; }
}  // namespace internal
}  // namespace ray
