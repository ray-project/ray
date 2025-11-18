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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"

namespace plasma {

class FakePlasmaClient : public PlasmaClientInterface {
 public:
  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name = "",
                 int num_retries = -1) override {
    return Status::OK();
  };

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                bool is_mutable,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                plasma::flatbuf::ObjectSource source,
                                int device_num = 0) override {
    return Status::OK();
  }

  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              std::shared_ptr<Buffer> *data,
                              plasma::flatbuf::ObjectSource source,
                              int device_num = 0) override {
    std::vector<uint8_t> data_vec(data_size);
    if (data != nullptr && data_size > 0) {
      data_vec.assign(data->get()->Data(), data->get()->Data() + data_size);
    }
    std::vector<uint8_t> metadata_vec;
    if (metadata != nullptr && metadata_size > 0) {
      metadata_vec.assign(metadata, metadata + metadata_size);
    }
    objects_in_plasma_.emplace(
        object_id, std::make_pair(std::move(data_vec), std::move(metadata_vec)));
    return Status::OK();
  }

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<plasma::ObjectBuffer> *object_buffers) override {
    object_buffers->reserve(object_ids.size());
    for (const auto &id : object_ids) {
      if (objects_in_plasma_.contains(id)) {
        auto &buffers = objects_in_plasma_[id];
        plasma::ObjectBuffer shm_buffer{
            std::make_shared<SharedMemoryBuffer>(buffers.first.data(),
                                                 buffers.first.size()),
            std::make_shared<SharedMemoryBuffer>(buffers.second.data(),
                                                 buffers.second.size())};
        object_buffers->emplace_back(shm_buffer);
      } else {
        object_buffers->emplace_back(plasma::ObjectBuffer{});
      }
    }
    return Status::OK();
  }

  Status GetExperimentalMutableObject(
      const ObjectID &object_id,
      std::unique_ptr<plasma::MutableObject> *mutable_object) override {
    return Status::OK();
  }

  Status Release(const ObjectID &object_id) override {
    objects_in_plasma_.erase(object_id);
    return Status::OK();
  }

  Status Contains(const ObjectID &object_id, bool *has_object) override {
    *has_object = objects_in_plasma_.contains(object_id);
    return Status::OK();
  }

  Status Abort(const ObjectID &object_id) override { return Status::OK(); }

  Status Seal(const ObjectID &object_id) override { return Status::OK(); }

  Status Delete(const std::vector<ObjectID> &object_ids) override {
    num_free_objects_requests++;
    for (const auto &id : object_ids) {
      objects_in_plasma_.erase(id);
    }
    return Status::OK();
  }

  void Disconnect() override{};

  std::string DebugString() { return ""; }

  StatusOr<std::string> GetMemoryUsage() override { return std::string("fake"); }

  absl::flat_hash_map<ObjectID, std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>
      objects_in_plasma_;
  uint32_t num_free_objects_requests = 0;
};

}  // namespace plasma
