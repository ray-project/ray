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

#include "gmock/gmock.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"

namespace ray {
namespace core {
namespace experimental {

class MockMutableObjectProvider : public MutableObjectProviderInterface {
 public:
  MOCK_METHOD(void, RegisterReaderChannel, (const ObjectID &object_id), (override));
  MOCK_METHOD(void,
              RegisterWriterChannel,
              (const ObjectID &writer_object_id,
               const std::vector<NodeID> &remote_reader_node_ids),
              (override));
  MOCK_METHOD(void,
              HandleRegisterMutableObject,
              (const ObjectID &writer_object_id,
               int64_t num_readers,
               const ObjectID &reader_object_id),
              (override));
  MOCK_METHOD(void,
              HandlePushMutableObject,
              (const rpc::PushMutableObjectRequest &request,
               rpc::PushMutableObjectReply *reply),
              (override));
  MOCK_METHOD(Status,
              WriteAcquire,
              (const ObjectID &object_id,
               int64_t data_size,
               const uint8_t *metadata,
               int64_t metadata_size,
               int64_t num_readers,
               std::shared_ptr<Buffer> &data,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(Status, WriteRelease, (const ObjectID &object_id), (override));
  MOCK_METHOD(Status,
              ReadAcquire,
              (const ObjectID &object_id,
               std::shared_ptr<RayObject> &result,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(Status, ReadRelease, (const ObjectID &object_id), (override));
  MOCK_METHOD(Status, SetError, (const ObjectID &object_id), (override));
  MOCK_METHOD(Status,
              GetChannelStatus,
              (const ObjectID &object_id, bool is_reader),
              (override));
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
