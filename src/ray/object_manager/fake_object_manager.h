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

#include <string>
#include <vector>

#include "ray/object_manager/object_manager_interface.h"

namespace ray {

// Hand-written fake for ObjectManagerInterface. Methods default to trivial
// return values; public fields let tests program return values and inspect
// recorded calls.
class FakeObjectManager : public ObjectManagerInterface {
 public:
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                BundlePriority prio,
                const TaskMetricsKey &task_key) override {
    pull_calls.push_back(object_refs);
    return next_pull_request_id++;
  }

  void CancelPull(uint64_t request_id) override {
    cancel_pull_calls.push_back(request_id);
  }

  void MarkObjectFailed(const ObjectID &object_id, rpc::ErrorType error_type) override {
    mark_object_failed_calls.push_back(object_id);
  }

  bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const override {
    return false;
  }

  int64_t PullManagerNumInactivePullsByTaskName(
      const TaskMetricsKey &task_key) const override {
    return 0;
  }

  int GetServerPort() const override { return 0; }

  void FreeObjects(const std::vector<ObjectID> &object_ids) override {
    for (const auto &id : object_ids) {
      freed_objects.push_back(id);
    }
  }

  bool IsPlasmaObjectSpillable(const ObjectID &object_id) override { return false; }

  int64_t GetUsedMemory() const override { return 0; }

  bool PullManagerHasPullsQueued() const override { return false; }

  int64_t GetMemoryCapacity() const override { return 0; }

  std::string DebugString() const override { return "FakeObjectManager"; }

  void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const override {}

  double GetUsedMemoryPercentage() const override { return 0.0; }

  void Stop() override {}

  void RecordMetrics() override {}

  void HandleNodeRemoved(const NodeID &node_id) override {
    handle_node_removed_calls.push_back(node_id);
  }

  std::vector<ObjectID> GetLocalObjectsOwnedBy(const WorkerID &worker_id) const override {
    return {};
  }

  std::vector<ObjectID> GetLocalObjectsOwnedByOwnersOn(
      const NodeID &node_id) const override {
    return {};
  }

  void HandleObjectAdded(const ObjectInfo &object_info) override {
    added_objects.push_back(object_info.object_id);
  }

  void HandleObjectDeleted(const ObjectID &object_id) override {
    deleted_objects.push_back(object_id);
  }

  // Programmable state / recorded calls.
  uint64_t next_pull_request_id = 1;
  std::vector<std::vector<rpc::ObjectReference>> pull_calls;
  std::vector<uint64_t> cancel_pull_calls;
  std::vector<ObjectID> mark_object_failed_calls;
  std::vector<ObjectID> freed_objects;
  std::vector<ObjectID> added_objects;
  std::vector<ObjectID> deleted_objects;
  std::vector<NodeID> handle_node_removed_calls;
};

}  // namespace ray
