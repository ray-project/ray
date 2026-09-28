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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs_rpc_client/accessor.h"

namespace ray {
namespace gcs {

// Hand-written fakes for the accessor interfaces. Each fake subclasses a real
// accessor base class and overrides exactly the methods tests exercise. The
// generic bodies:
//   - record the salient arguments of each call in a public vector (or bump a
//     public counter when the args aren't interesting),
//   - stash the last callback passed to async methods in a public member so
//     tests can drive completion manually,
//   - return a public, settable `Status` field (default `Status::OK()`) for
//     synchronous methods that can fail,
//   - return public, settable state for pure getters.
// Async methods never invoke their callbacks on their own; a test is expected to
// invoke the stored callback when it wants the completion to fire.

class FakeJobInfoAccessor : public JobInfoAccessor {
 public:
  void AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                const rpc::StatusCallback &callback) override {
    async_add_calls.push_back(data_ptr);
    async_add_callback = callback;
  }

  void AsyncMarkFinished(const JobID &job_id,
                         const rpc::StatusCallback &callback) override {
    async_mark_finished_calls.push_back(job_id);
    async_mark_finished_callback = callback;
  }

  void AsyncSubscribeAll(
      const rpc::SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const rpc::StatusCallback &done) override {
    async_subscribe_all_call_count++;
    async_subscribe_all_subscribe = subscribe;
    async_subscribe_all_done = done;
  }

  void AsyncGetAll(const std::optional<std::string> &job_or_submission_id,
                   bool skip_submission_job_info_field,
                   bool skip_is_running_tasks_field,
                   const rpc::MultiItemCallback<rpc::JobTableData> &callback,
                   int64_t timeout_ms) override {
    async_get_all_calls.push_back(job_or_submission_id);
    async_get_all_callback = callback;
  }

  void AsyncResubscribe() override { async_resubscribe_call_count++; }

  void AsyncGetNextJobID(const rpc::ItemCallback<JobID> &callback) override {
    async_get_next_job_id_callback = callback;
  }

  std::vector<std::shared_ptr<rpc::JobTableData>> async_add_calls;
  rpc::StatusCallback async_add_callback;
  std::vector<JobID> async_mark_finished_calls;
  rpc::StatusCallback async_mark_finished_callback;
  int async_subscribe_all_call_count = 0;
  rpc::SubscribeCallback<JobID, rpc::JobTableData> async_subscribe_all_subscribe;
  rpc::StatusCallback async_subscribe_all_done;
  std::vector<std::optional<std::string>> async_get_all_calls;
  rpc::MultiItemCallback<rpc::JobTableData> async_get_all_callback;
  int async_resubscribe_call_count = 0;
  rpc::ItemCallback<JobID> async_get_next_job_id_callback;
};

class FakeNodeInfoAccessor : public NodeInfoAccessor {
 public:
  void RegisterSelf(rpc::GcsNodeInfo &&local_node_info,
                    const rpc::StatusCallback &callback) override {
    register_self_calls.push_back(std::move(local_node_info));
    register_self_callback = callback;
  }

  void AsyncRegister(const rpc::GcsNodeInfo &node_info,
                     const rpc::StatusCallback &callback) override {
    async_register_calls.push_back(node_info);
    async_register_callback = callback;
  }

  void AsyncCheckAlive(const std::vector<NodeID> &node_ids,
                       int64_t timeout_ms,
                       const rpc::MultiItemCallback<bool> &callback) override {
    async_check_alive_calls.push_back(node_ids);
    async_check_alive_callback = callback;
    if (async_check_alive_hook) {
      async_check_alive_hook(node_ids, timeout_ms, callback);
    }
  }

  void AsyncGetAll(
      const rpc::OptionalItemCallback<std::pair<std::vector<rpc::GcsNodeInfo>, int64_t>>
          &callback,
      int64_t timeout_ms,
      const std::optional<rpc::GcsNodeInfo::GcsNodeState> &state_filter,
      const std::vector<rpc::GetAllNodeInfoRequest::NodeSelector> &node_selectors,
      const std::optional<int64_t> &limit) const override {
    async_get_all_call_count++;
    async_get_all_callback = callback;
  }

  void AsyncGetAllNodeAddressAndLiveness(
      const rpc::MultiItemCallback<rpc::GcsNodeAddressAndLiveness> &callback,
      int64_t timeout_ms,
      const std::vector<NodeID> &node_ids) override {
    async_get_all_node_address_and_liveness_callback = callback;
  }

  void AsyncSubscribeToNodeAddressAndLivenessChange(
      std::function<void(NodeID, const rpc::GcsNodeAddressAndLiveness &)> subscribe,
      rpc::StatusCallback done) override {
    async_subscribe_to_node_address_and_liveness_change_call_count++;
    node_address_and_liveness_subscribe = std::move(subscribe);
    node_address_and_liveness_done = std::move(done);
  }

  std::optional<rpc::GcsNodeAddressAndLiveness> GetNodeAddressAndLiveness(
      const NodeID &node_id, bool filter_dead_nodes) const override {
    auto it = node_address_and_liveness.find(node_id);
    if (it == node_address_and_liveness.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness>
  GetAllNodeAddressAndLiveness() const override {
    return node_address_and_liveness;
  }

  Status CheckAlive(const std::vector<NodeID> &node_ids,
                    int64_t timeout_ms,
                    std::vector<bool> &nodes_alive) override {
    check_alive_calls.push_back(node_ids);
    nodes_alive = check_alive_result;
    return check_alive_status;
  }

  bool IsNodeDead(const NodeID &node_id) const override {
    return dead_nodes.contains(node_id);
  }

  bool IsNodeAlive(const NodeID &node_id) const override {
    return alive_nodes.contains(node_id);
  }

  void AsyncResubscribe() override { async_resubscribe_call_count++; }

  std::vector<rpc::GcsNodeInfo> register_self_calls;
  rpc::StatusCallback register_self_callback;
  std::vector<rpc::GcsNodeInfo> async_register_calls;
  rpc::StatusCallback async_register_callback;
  std::vector<std::vector<NodeID>> async_check_alive_calls;
  rpc::MultiItemCallback<bool> async_check_alive_callback;
  // Optional hook to drive the AsyncCheckAlive callback / observe the call inline.
  std::function<void(
      const std::vector<NodeID> &, int64_t, const rpc::MultiItemCallback<bool> &)>
      async_check_alive_hook;
  int async_subscribe_to_node_address_and_liveness_change_call_count = 0;
  mutable int async_get_all_call_count = 0;
  mutable rpc::OptionalItemCallback<std::pair<std::vector<rpc::GcsNodeInfo>, int64_t>>
      async_get_all_callback;
  rpc::MultiItemCallback<rpc::GcsNodeAddressAndLiveness>
      async_get_all_node_address_and_liveness_callback;
  std::function<void(NodeID, const rpc::GcsNodeAddressAndLiveness &)>
      node_address_and_liveness_subscribe;
  rpc::StatusCallback node_address_and_liveness_done;
  // Backing store for the local-cache getters; populate to control return values.
  absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness> node_address_and_liveness;
  std::vector<std::vector<NodeID>> check_alive_calls;
  std::vector<bool> check_alive_result;
  Status check_alive_status = Status::OK();
  absl::flat_hash_set<NodeID> dead_nodes;
  absl::flat_hash_set<NodeID> alive_nodes;
  int async_resubscribe_call_count = 0;
};

class FakeNodeResourceInfoAccessor : public NodeResourceInfoAccessor {
 public:
  void AsyncGetAllAvailableResources(
      const rpc::MultiItemCallback<rpc::AvailableResources> &callback) override {
    async_get_all_available_resources_callback = callback;
  }

  void AsyncGetAllResourceUsage(
      const rpc::ItemCallback<rpc::ResourceUsageBatchData> &callback) override {
    async_get_all_resource_usage_callback = callback;
  }

  rpc::MultiItemCallback<rpc::AvailableResources>
      async_get_all_available_resources_callback;
  rpc::ItemCallback<rpc::ResourceUsageBatchData> async_get_all_resource_usage_callback;
};

class FakeErrorInfoAccessor : public ErrorInfoAccessor {
 public:
  void AsyncReportJobError(rpc::ErrorTableData data) override {
    async_report_job_error_calls.push_back(std::move(data));
  }

  std::vector<rpc::ErrorTableData> async_report_job_error_calls;
};

class FakeTaskInfoAccessor : public TaskInfoAccessor {
 public:
  void AsyncAddTaskEventData(std::unique_ptr<rpc::TaskEventData> data_ptr,
                             rpc::StatusCallback callback) override {
    async_add_task_event_data_call_count++;
    if (async_add_task_event_data_hook) {
      async_add_task_event_data_hook(std::move(data_ptr), std::move(callback));
      return;
    }
    async_add_task_event_data_calls.push_back(std::move(data_ptr));
    async_add_task_event_data_callback = std::move(callback);
  }

  std::vector<std::unique_ptr<rpc::TaskEventData>> async_add_task_event_data_calls;
  rpc::StatusCallback async_add_task_event_data_callback;
  int async_add_task_event_data_call_count = 0;
  // Optional hook to observe data / drive the completion callback inline.
  std::function<void(std::unique_ptr<rpc::TaskEventData>, rpc::StatusCallback)>
      async_add_task_event_data_hook;
};

class FakeWorkerInfoAccessor : public WorkerInfoAccessor {
 public:
  void AsyncSubscribeToWorkerFailures(
      const rpc::ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const rpc::StatusCallback &done) override {
    async_subscribe_to_worker_failures_call_count++;
    async_subscribe_to_worker_failures_subscribe = subscribe;
    async_subscribe_to_worker_failures_done = done;
  }

  void AsyncSubscribeToWorkerFailure(
      const WorkerID &worker_id,
      const rpc::ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const rpc::StatusCallback &done) override {
    async_subscribe_to_worker_failure_calls.push_back(worker_id);
    async_subscribe_to_worker_failure_subscribe = subscribe;
    async_subscribe_to_worker_failure_done = done;
  }

  void AsyncUnsubscribeFromWorkerFailure(const WorkerID &worker_id) override {
    async_unsubscribe_from_worker_failure_calls.push_back(worker_id);
  }

  void AsyncReportWorkerFailure(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                                const rpc::StatusCallback &callback) override {
    async_report_worker_failure_calls.push_back(data_ptr);
    async_report_worker_failure_callback = callback;
  }

  void AsyncGet(
      const WorkerID &worker_id,
      const rpc::OptionalItemCallback<rpc::WorkerTableData> &callback) override {
    async_get_calls.push_back(worker_id);
    async_get_callback = callback;
    if (async_get_hook) {
      async_get_hook(worker_id, callback);
    }
  }

  void AsyncGetAll(
      const rpc::MultiItemCallback<rpc::WorkerTableData> &callback) override {
    async_get_all_callback = callback;
  }

  void AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                const rpc::StatusCallback &callback) override {
    async_add_calls.push_back(data_ptr);
    async_add_callback = callback;
  }

  void AsyncResubscribe() override { async_resubscribe_call_count++; }

  int async_subscribe_to_worker_failures_call_count = 0;
  rpc::ItemCallback<rpc::WorkerDeltaData> async_subscribe_to_worker_failures_subscribe;
  rpc::StatusCallback async_subscribe_to_worker_failures_done;
  std::vector<WorkerID> async_subscribe_to_worker_failure_calls;
  rpc::ItemCallback<rpc::WorkerDeltaData> async_subscribe_to_worker_failure_subscribe;
  rpc::StatusCallback async_subscribe_to_worker_failure_done;
  std::vector<WorkerID> async_unsubscribe_from_worker_failure_calls;
  std::vector<std::shared_ptr<rpc::WorkerTableData>> async_report_worker_failure_calls;
  rpc::StatusCallback async_report_worker_failure_callback;
  std::vector<WorkerID> async_get_calls;
  rpc::OptionalItemCallback<rpc::WorkerTableData> async_get_callback;
  // Optional synchronous hook to drive the AsyncGet callback inline.
  std::function<void(const WorkerID &,
                     const rpc::OptionalItemCallback<rpc::WorkerTableData> &)>
      async_get_hook;
  rpc::MultiItemCallback<rpc::WorkerTableData> async_get_all_callback;
  std::vector<std::shared_ptr<rpc::WorkerTableData>> async_add_calls;
  rpc::StatusCallback async_add_callback;
  int async_resubscribe_call_count = 0;
};

class FakePlacementGroupInfoAccessor : public PlacementGroupInfoAccessor {
 public:
  Status SyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec) override {
    sync_create_placement_group_call_count++;
    return sync_create_placement_group_status;
  }

  void AsyncGet(
      const PlacementGroupID &placement_group_id,
      const rpc::OptionalItemCallback<rpc::PlacementGroupTableData> &callback) override {
    async_get_calls.push_back(placement_group_id);
    async_get_callback = callback;
  }

  void AsyncGetByName(
      const std::string &placement_group_name,
      const std::string &ray_namespace,
      const rpc::OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
      int64_t timeout_ms) override {
    async_get_by_name_calls.emplace_back(placement_group_name, ray_namespace);
    async_get_by_name_callback = callback;
  }

  void AsyncGetAll(
      const rpc::MultiItemCallback<rpc::PlacementGroupTableData> &callback) override {
    async_get_all_callback = callback;
  }

  Status SyncRemovePlacementGroup(const PlacementGroupID &placement_group_id) override {
    sync_remove_placement_group_calls.push_back(placement_group_id);
    return sync_remove_placement_group_status;
  }

  Status SyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                            int64_t timeout_seconds) override {
    sync_wait_until_ready_calls.push_back(placement_group_id);
    return sync_wait_until_ready_status;
  }

  int sync_create_placement_group_call_count = 0;
  Status sync_create_placement_group_status = Status::OK();
  std::vector<PlacementGroupID> async_get_calls;
  rpc::OptionalItemCallback<rpc::PlacementGroupTableData> async_get_callback;
  std::vector<std::pair<std::string, std::string>> async_get_by_name_calls;
  rpc::OptionalItemCallback<rpc::PlacementGroupTableData> async_get_by_name_callback;
  rpc::MultiItemCallback<rpc::PlacementGroupTableData> async_get_all_callback;
  std::vector<PlacementGroupID> sync_remove_placement_group_calls;
  Status sync_remove_placement_group_status = Status::OK();
  std::vector<PlacementGroupID> sync_wait_until_ready_calls;
  Status sync_wait_until_ready_status = Status::OK();
};

class FakeInternalKVAccessor : public InternalKVAccessor {
 public:
  void AsyncInternalKVKeys(
      const std::string &ns,
      const std::string &prefix,
      const int64_t timeout_ms,
      const rpc::OptionalItemCallback<std::vector<std::string>> &callback) override {
    async_internal_kv_keys_calls.emplace_back(ns, prefix);
    async_internal_kv_keys_callback = callback;
  }

  void AsyncInternalKVGet(
      const std::string &ns,
      const std::string &key,
      const int64_t timeout_ms,
      const rpc::OptionalItemCallback<std::string> &callback) override {
    async_internal_kv_get_calls.emplace_back(ns, key);
    async_internal_kv_get_callback = callback;
  }

  void AsyncInternalKVPut(const std::string &ns,
                          const std::string &key,
                          const std::string &value,
                          bool overwrite,
                          const int64_t timeout_ms,
                          const rpc::OptionalItemCallback<bool> &callback) override {
    async_internal_kv_put_calls.emplace_back(ns, key, value);
    async_internal_kv_put_callback = callback;
  }

  void AsyncInternalKVExists(const std::string &ns,
                             const std::string &key,
                             const int64_t timeout_ms,
                             const rpc::OptionalItemCallback<bool> &callback) override {
    async_internal_kv_exists_calls.emplace_back(ns, key);
    async_internal_kv_exists_callback = callback;
  }

  void AsyncInternalKVDel(const std::string &ns,
                          const std::string &key,
                          bool del_by_prefix,
                          const int64_t timeout_ms,
                          const rpc::OptionalItemCallback<int> &callback) override {
    async_internal_kv_del_calls.emplace_back(ns, key);
    async_internal_kv_del_callback = callback;
  }

  void AsyncGetInternalConfig(
      const rpc::OptionalItemCallback<std::string> &callback) override {
    async_get_internal_config_callback = callback;
  }

  std::vector<std::pair<std::string, std::string>> async_internal_kv_keys_calls;
  rpc::OptionalItemCallback<std::vector<std::string>> async_internal_kv_keys_callback;
  std::vector<std::pair<std::string, std::string>> async_internal_kv_get_calls;
  rpc::OptionalItemCallback<std::string> async_internal_kv_get_callback;
  std::vector<std::tuple<std::string, std::string, std::string>>
      async_internal_kv_put_calls;
  rpc::OptionalItemCallback<bool> async_internal_kv_put_callback;
  std::vector<std::pair<std::string, std::string>> async_internal_kv_exists_calls;
  rpc::OptionalItemCallback<bool> async_internal_kv_exists_callback;
  std::vector<std::pair<std::string, std::string>> async_internal_kv_del_calls;
  rpc::OptionalItemCallback<int> async_internal_kv_del_callback;
  rpc::OptionalItemCallback<std::string> async_get_internal_config_callback;
};

}  // namespace gcs
}  // namespace ray
