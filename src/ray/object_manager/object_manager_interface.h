// Copyright 2017 The Ray Authors.
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
#include <vector>

#include "ray/common/id.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/pull_manager.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

class ObjectManagerInterface {
 public:
  virtual uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                        BundlePriority prio,
                        const TaskMetricsKey &task_key) = 0;
  virtual void CancelPull(uint64_t request_id) = 0;
  /// Mark the specified object as failed with the given error type.
  ///
  /// \param object_id The object id to store error message into.
  /// \param error_type The type of the error that caused this task to fail.
  virtual void MarkObjectFailed(const ObjectID &object_id, rpc::ErrorType error_type) = 0;
  virtual bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const = 0;
  virtual int64_t PullManagerNumInactivePullsByTaskName(
      const TaskMetricsKey &task_key) const = 0;
  virtual int GetServerPort() const = 0;
  virtual void FreeObjects(const std::vector<ObjectID> &object_ids) = 0;
  virtual void HandleNodeRemoved(const NodeID &node_id) = 0;
  virtual std::vector<ObjectID> GetLocalObjectsOwnedBy(
      const WorkerID &worker_id) const = 0;
  virtual std::vector<ObjectID> GetLocalObjectsOwnedByOwnersOn(
      const NodeID &node_id) const = 0;
  virtual bool IsPlasmaObjectSpillable(const ObjectID &object_id) = 0;
  virtual int64_t GetUsedMemory() const = 0;
  virtual bool PullManagerHasPullsQueued() const = 0;
  virtual int64_t GetMemoryCapacity() const = 0;
  virtual std::string DebugString() const = 0;
  virtual void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const = 0;
  virtual double GetUsedMemoryPercentage() const = 0;
  virtual void Stop() = 0;
  virtual void RecordMetrics() = 0;
  virtual void HandleObjectAdded(const ObjectInfo &object_info) = 0;
  virtual void HandleObjectDeleted(const ObjectID &object_id) = 0;

  virtual ~ObjectManagerInterface() = default;
};

}  // namespace ray
