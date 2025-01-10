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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace gcs {

/// \class GlobalStateAccessor
///
/// `GlobalStateAccessor` is used to provide synchronous interfaces to access data in GCS
/// for the language front-end (e.g., Python's `state.py`).
class GlobalStateAccessor {
 public:
  /// Constructor of GlobalStateAccessor.
  ///
  /// \param gcs_client_options The client options to connect to gcs
  explicit GlobalStateAccessor(const GcsClientOptions &gcs_client_options);

  ~GlobalStateAccessor() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Connect gcs server.
  ///
  /// \return Whether the connection is successful.
  bool Connect() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Disconnect from gcs server.
  void Disconnect() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of all jobs from GCS Service.
  ///
  /// \return All job info. To support multi-language, we serialize each JobTableData and
  /// return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::vector<std::string> GetAllJobInfo(bool skip_submission_job_info_field = false,
                                         bool skip_is_running_tasks_field = false)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get next job id from GCS Service.
  ///
  /// \return Next job id.
  JobID GetNextJobID() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get all node information from GCS.
  ///
  /// \return A list of `GcsNodeInfo` objects serialized in protobuf format.
  std::vector<std::string> GetAllNodeInfo() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of all task events from GCS Service.
  ///
  /// \return All task events info.
  std::vector<std::string> GetAllTaskEvents() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get available resources of all nodes.
  ///
  /// \return available resources of all nodes. To support multi-language, we serialize
  /// each AvailableResources and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllAvailableResources() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get total resources of all nodes.
  ///
  /// \return total resources of all nodes. To support multi-language, we serialize
  /// each TotalResources and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllTotalResources() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get draining nodes.
  ///
  /// \return Draining node id to draining deadline.
  std::unordered_map<NodeID, int64_t> GetDrainingNodes() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get newest resource usage of all nodes from GCS Service. Only used when light
  /// rerouce usage report enabled.
  ///
  /// \return resource usage info. To support multi-language, we serialize each
  /// data and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetAllResourceUsage() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of all actors from GCS Service.
  ///
  /// \param  actor_id To filter actors by actor_id.
  /// \param  job_id To filter actors by job_id.
  /// \param  actor_state_name To filter actors based on actor state.
  /// \return All actor info. To support multi-language, we serialize each ActorTableData
  /// and return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::vector<std::string> GetAllActorInfo(
      const std::optional<ActorID> &actor_id = std::nullopt,
      const std::optional<JobID> &job_id = std::nullopt,
      const std::optional<std::string> &actor_state_name = std::nullopt)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of an actor from GCS Service.
  ///
  /// \param actor_id The ID of actor to look up in the GCS Service.
  /// \return Actor info. To support multi-language, we serialize each ActorTableData and
  /// return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::unique_ptr<std::string> GetActorInfo(const ActorID &actor_id)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of a worker from GCS Service.
  ///
  /// \param worker_id The ID of worker to look up in the GCS Service.
  /// \return Worker info. To support multi-language, we serialize each WorkerTableData
  /// and return the serialized string. Where used, it needs to be deserialized with
  /// protobuf function.
  std::unique_ptr<std::string> GetWorkerInfo(const WorkerID &worker_id)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of all workers from GCS Service.
  ///
  /// \return All worker info. To support multi-language, we serialize each
  /// WorkerTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllWorkerInfo() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get the worker debugger port from the GCS Service.
  ///
  /// \param worker_id The ID of worker to look up in the GCS Service.
  /// \return The worker debugger port.
  uint32_t GetWorkerDebuggerPort(const WorkerID &worker_id);

  /// Update the worker debugger port in the GCS Service.
  ///
  /// \param worker_id The ID of worker to update in the GCS Service.
  /// \param debugger_port The debugger port of worker to update in the GCS Service.
  /// \return Is operation success.
  bool UpdateWorkerDebuggerPort(const WorkerID &worker_id, const uint32_t debugger_port);

  /// Update the worker num of paused threads in the GCS Service.
  ///
  /// \param worker_id The ID of worker to update in the GCS Service.
  /// \param num_paused_threads_delta The delta of paused threads of worker to update in
  /// the GCS Service. \return Is operation success.
  bool UpdateWorkerNumPausedThreads(const WorkerID &worker_id,
                                    const int num_paused_threads_delta);

  /// Add information of a worker to GCS Service.
  ///
  /// \param serialized_string The serialized data of worker to be added in the GCS
  /// Service, use string is convenient for python to use.
  /// \return Is operation success.
  bool AddWorkerInfo(const std::string &serialized_string) ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of all placement group from GCS Service.
  ///
  /// \return All placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::vector<std::string> GetAllPlacementGroupInfo() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of a placement group from GCS Service by ID.
  ///
  /// \param placement_group_id The ID of placement group to look up in the GCS Service.
  /// \return Placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetPlacementGroupInfo(
      const PlacementGroupID &placement_group_id) ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get information of a placement group from GCS Service by name.
  ///
  /// \param placement_group_name The name of placement group to look up in the GCS
  /// Service.
  /// \return Placement group info. To support multi-language, we serialize each
  /// PlacementGroupTableData and return the serialized string. Where used, it needs to be
  /// deserialized with protobuf function.
  std::unique_ptr<std::string> GetPlacementGroupByName(
      const std::string &placement_group_name, const std::string &ray_namespace)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get value of the key from GCS Service.
  ///
  /// \param ns namespace to get.
  /// \param key key to get.
  /// \return Value of the key.
  std::unique_ptr<std::string> GetInternalKV(const std::string &ns,
                                             const std::string &key)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get the serialized system config from GCS.
  ///
  /// \return The serialized system config.
  std::string GetSystemConfig() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get the node with the specified node ID.
  ///
  /// \param[in] node_id The hex string format of the node ID.
  /// \param[out] node_info The output parameter to store the node info. To support
  /// multi-language, we serialize each GcsNodeInfo and return the serialized string.
  /// Where used, it needs to be deserialized with protobuf function.
  ray::Status GetNode(const std::string &node_id, std::string *node_info)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get the node to connect for a Ray driver.
  ///
  /// \param[in] node_ip_address The IP address of the desired node to connect.
  /// \param[out] node_to_connect The info of the node to connect. To support
  /// multi-language, we serialize each GcsNodeInfo and return the serialized string.
  /// Where used, it needs to be deserialized with protobuf function.
  ray::Status GetNodeToConnectForDriver(const std::string &node_ip_address,
                                        std::string *node_to_connect)
      ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  /// Synchronously get the current alive nodes from GCS Service.
  ///
  /// \param[out] nodes The output parameter to store the alive nodes.
  ray::Status GetAliveNodes(std::vector<rpc::GcsNodeInfo> &nodes)
      ABSL_LOCKS_EXCLUDED(mutex_);

  /// MultiItem transformation helper in template style.
  ///
  /// \return MultiItemCallback within in rpc type DATA.
  template <class DATA>
  MultiItemCallback<DATA> TransformForMultiItemCallback(
      std::vector<std::string> &data_vec, std::promise<bool> &promise) {
    return [&data_vec, &promise](const Status &status, std::vector<DATA> &&result) {
      RAY_CHECK_OK(status);
      std::transform(result.begin(),
                     result.end(),
                     std::back_inserter(data_vec),
                     [](const DATA &data) { return data.SerializeAsString(); });
      promise.set_value(true);
    };
  }

  /// OptionalItem transformation helper in template style.
  ///
  /// \return OptionalItemCallback within in rpc type DATA.
  template <class DATA>
  OptionalItemCallback<DATA> TransformForOptionalItemCallback(
      std::unique_ptr<std::string> &data, std::promise<bool> &promise) {
    return [&data, &promise](const Status &status, const std::optional<DATA> &result) {
      RAY_CHECK_OK(status);
      if (result) {
        data.reset(new std::string(result->SerializeAsString()));
      }
      promise.set_value(true);
    };
  }

  /// Item transformation helper in template style.
  ///
  /// \return ItemCallback within in rpc type DATA.
  template <class DATA>
  ItemCallback<DATA> TransformForItemCallback(std::unique_ptr<std::string> &data,
                                              std::promise<bool> &promise) {
    return [&data, &promise](const DATA &result) {
      data.reset(new std::string(result.SerializeAsString()));
      promise.set_value(true);
    };
  }

  std::string redis_address_;
  std::string redis_ip_address_;

  // protects is_connected_ and gcs_client_
  mutable absl::Mutex mutex_;

  // protects debugger port related operations
  mutable absl::Mutex debugger_port_mutex_;

  // protects debugger tasks related operations
  mutable absl::Mutex debugger_threads_mutex_;

  /// Whether this client is connected to gcs server.
  bool is_connected_ ABSL_GUARDED_BY(mutex_) = false;

  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<instrumented_io_context> io_service_;
  std::unique_ptr<GcsClient> gcs_client_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray
