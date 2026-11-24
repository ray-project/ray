// Copyright The Ray Authors.
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
#include <vector>

#include "ray/core_worker/experimental_mutable_object_provider_interface.h"

namespace ray {
namespace core {
namespace experimental {

// Hand-written fake for MutableObjectProviderInterface. All methods default to
// no-op / Status::OK(); public fields record calls for tests to assert on.
class FakeMutableObjectProvider : public MutableObjectProviderInterface {
 public:
  void RegisterReaderChannel(const ObjectID &object_id) override {
    registered_reader_channels.push_back(object_id);
  }

  void RegisterWriterChannel(const ObjectID &writer_object_id,
                             const std::vector<NodeID> &remote_reader_node_ids) override {
    registered_writer_channels.push_back(writer_object_id);
  }

  void HandleRegisterMutableObject(const ObjectID &writer_object_id,
                                   int64_t num_readers,
                                   const ObjectID &reader_object_id) override {}

  void HandlePushMutableObject(const rpc::PushMutableObjectRequest &request,
                               rpc::PushMutableObjectReply *reply) override {}

  Status WriteAcquire(const ObjectID &object_id,
                      int64_t data_size,
                      const uint8_t *metadata,
                      int64_t metadata_size,
                      int64_t num_readers,
                      std::shared_ptr<Buffer> &data,
                      int64_t timeout_ms) override {
    return Status::OK();
  }

  Status WriteRelease(const ObjectID &object_id) override { return Status::OK(); }

  Status ReadAcquire(const ObjectID &object_id,
                     std::shared_ptr<RayObject> &result,
                     int64_t timeout_ms) override {
    return Status::OK();
  }

  Status ReadRelease(const ObjectID &object_id) override { return Status::OK(); }

  Status SetError(const ObjectID &object_id) override { return Status::OK(); }

  Status GetChannelStatus(const ObjectID &object_id, bool is_reader) override {
    return Status::OK();
  }

  std::vector<ObjectID> registered_reader_channels;
  std::vector<ObjectID> registered_writer_channels;
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
