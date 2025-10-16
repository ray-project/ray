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
#include "ray/object_manager/object_directory.h"

namespace ray {

class MockObjectDirectory : public IObjectDirectory {
 public:
  MOCK_METHOD(void, HandleNodeRemoved, (const NodeID &node_id), (override));

  MOCK_METHOD(void,
              SubscribeObjectLocations,
              (const UniqueID &callback_id,
               const ObjectID &object_id,
               const rpc::Address &owner_address,
               const OnLocationsFound &callback),
              (override));

  MOCK_METHOD(void,
              UnsubscribeObjectLocations,
              (const UniqueID &callback_id, const ObjectID &object_id),
              (override));

  MOCK_METHOD(void,
              ReportObjectAdded,
              (const ObjectID &object_id,
               const NodeID &node_id,
               const ObjectInfo &object_info),
              (override));

  MOCK_METHOD(void,
              ReportObjectRemoved,
              (const ObjectID &object_id,
               const NodeID &node_id,
               const ObjectInfo &object_info),
              (override));

  MOCK_METHOD(void,
              ReportObjectSpilled,
              (const ObjectID &object_id,
               const NodeID &node_id,
               const rpc::Address &owner_address,
               const std::string &spilled_url,
               const ObjectID &generator_id,
               const bool spilled_to_local_storage),
              (override));

  MOCK_METHOD(void, RecordMetrics, (uint64_t duration_ms), (override));

  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace ray
