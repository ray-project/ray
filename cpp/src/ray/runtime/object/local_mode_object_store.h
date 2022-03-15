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

#pragma once

#include <unordered_map>

#include "../local_mode_ray_runtime.h"
#include "object_store.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerMemoryStore;

class LocalModeObjectStore : public ObjectStore {
 public:
  LocalModeObjectStore(LocalModeRayRuntime &local_mode_ray_tuntime);

  std::vector<bool> Wait(const std::vector<ObjectID> &ids,
                         int num_objects,
                         int timeout_ms);

  void AddLocalReference(const std::string &id);

  void RemoveLocalReference(const std::string &id);

 private:
  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id, int timeout_ms);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(const std::vector<ObjectID> &ids,
                                                        int timeout_ms);

  std::unique_ptr<CoreWorkerMemoryStore> memory_store_;

  LocalModeRayRuntime &local_mode_ray_tuntime_;
};

}  // namespace internal
}  // namespace ray
