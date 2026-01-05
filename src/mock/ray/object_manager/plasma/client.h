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
#include "ray/object_manager/plasma/client.h"

namespace plasma {

class MockPlasmaClient : public PlasmaClientInterface {
 public:
  MOCK_METHOD(Status,
              Connect,
              (const std::string &store_socket_name,
               const std::string &manager_socket_name,
               int num_retries),
              (override));

  MOCK_METHOD(Status, Release, (const ObjectID &object_id), (override));

  MOCK_METHOD(Status,
              Contains,
              (const ObjectID &object_id, bool *has_object),
              (override));

  MOCK_METHOD(void, Disconnect, (), (override));

  MOCK_METHOD(Status,
              Get,
              (const std::vector<ObjectID> &object_ids,
               int64_t timeout_ms,
               std::vector<ObjectBuffer> *object_buffers),
              (override));

  MOCK_METHOD(Status,
              GetExperimentalMutableObject,
              (const ObjectID &object_id, std::unique_ptr<MutableObject> *mutable_object),
              (override));

  MOCK_METHOD(Status, Seal, (const ObjectID &object_id), (override));

  MOCK_METHOD(Status, Abort, (const ObjectID &object_id), (override));

  MOCK_METHOD(Status,
              CreateAndSpillIfNeeded,
              (const ObjectID &object_id,
               const ray::rpc::Address &owner_address,
               bool is_mutable,
               int64_t data_size,
               const uint8_t *metadata,
               int64_t metadata_size,
               std::shared_ptr<Buffer> *data,
               plasma::flatbuf::ObjectSource source,
               int device_num),
              (override));

  MOCK_METHOD(Status,
              TryCreateImmediately,
              (const ObjectID &object_id,
               const ray::rpc::Address &owner_address,
               int64_t data_size,
               const uint8_t *metadata,
               int64_t metadata_size,
               std::shared_ptr<Buffer> *data,
               plasma::flatbuf::ObjectSource source,
               int device_num),
              (override));

  MOCK_METHOD(Status, Delete, (const std::vector<ObjectID> &object_ids), (override));

  MOCK_METHOD(StatusOr<std::string>, GetMemoryUsage, (), (override));
};

}  // namespace plasma
