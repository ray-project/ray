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
#include "ray/object_manager/object_manager.h"

namespace ray {

class MockObjectManager : public ObjectManagerInterface {
 public:
  MOCK_METHOD(uint64_t,
              Pull,
              (const std::vector<rpc::ObjectReference> &object_refs,
               BundlePriority prio,
               const TaskMetricsKey &task_key),
              (override));
  MOCK_METHOD(void, CancelPull, (uint64_t request_id), (override));
  MOCK_METHOD(bool,
              PullRequestActiveOrWaitingForMetadata,
              (uint64_t request_id),
              (const, override));
  MOCK_METHOD(int64_t,
              PullManagerNumInactivePullsByTaskName,
              (const TaskMetricsKey &task_key),
              (const, override));
  MOCK_METHOD(int, GetServerPort, (), (const, override));
  MOCK_METHOD(void,
              FreeObjects,
              (const std::vector<ObjectID> &object_ids, bool local_only),
              (override));
  MOCK_METHOD(bool, IsPlasmaObjectSpillable, (const ObjectID &object_id), (override));
  MOCK_METHOD(int64_t, GetUsedMemory, (), (const, override));
  MOCK_METHOD(bool, PullManagerHasPullsQueued, (), (const, override));
  MOCK_METHOD(int64_t, GetMemoryCapacity, (), (const, override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
  MOCK_METHOD(void,
              FillObjectStoreStats,
              (rpc::GetNodeStatsReply * reply),
              (const, override));
  MOCK_METHOD(double, GetUsedMemoryPercentage, (), (const, override));
  MOCK_METHOD(void, Stop, (), (override));
  MOCK_METHOD(void, RecordMetrics, (), (override));
  MOCK_METHOD(void, HandleNodeRemoved, (const NodeID &node_id), (override));
  MOCK_METHOD(void, HandleObjectAdded, (const ObjectInfo &object_info), (override));
  MOCK_METHOD(void, HandleObjectDeleted, (const ObjectID &object_id), (override));
};

}  // namespace ray
