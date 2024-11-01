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

#include <gtest/gtest_prod.h>

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_split.h"
#include "gtest/gtest_prod.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"

namespace ray {

namespace gcs {

/// \class GcsClientOptions
/// GCS client's options (configuration items), such as service address, and service
/// password.
// TODO(ryw): eventually we will always have fetch_cluster_id_if_nil = true.
class GcsClientOptions {
 public:
  GcsClientOptions(const std::string &gcs_address,
                   int port,
                   const ClusterID &cluster_id,
                   bool allow_cluster_id_nil,
                   bool fetch_cluster_id_if_nil)
      : gcs_address_(gcs_address),
        gcs_port_(port),
        cluster_id_(cluster_id),
        should_fetch_cluster_id_(ShouldFetchClusterId(
            cluster_id, allow_cluster_id_nil, fetch_cluster_id_if_nil)) {}

  /// Constructor of GcsClientOptions from gcs address
  ///
  /// \param gcs_address gcs address, including port
  GcsClientOptions(const std::string &gcs_address,
                   const ClusterID &cluster_id,
                   bool allow_cluster_id_nil,
                   bool fetch_cluster_id_if_nil)
      : cluster_id_(cluster_id),
        should_fetch_cluster_id_(ShouldFetchClusterId(
            cluster_id, allow_cluster_id_nil, fetch_cluster_id_if_nil)) {
    std::vector<std::string> address = absl::StrSplit(gcs_address, ':');
    RAY_LOG(DEBUG) << "Connect to gcs server via address: " << gcs_address;
    RAY_CHECK(address.size() == 2);
    gcs_address_ = address[0];
    gcs_port_ = std::stoi(address[1]);
  }

  GcsClientOptions() {}

  // - CHECK-fails if invalid (cluster_id_ is nil but !allow_cluster_id_nil_)
  // - Returns false if no need to fetch (cluster_id_ is not nil, or
  //    !fetch_cluster_id_if_nil_).
  // - Returns true if needs to fetch.
  static bool ShouldFetchClusterId(ClusterID cluster_id,
                                   bool allow_cluster_id_nil,
                                   bool fetch_cluster_id_if_nil);

  // Gcs address
  std::string gcs_address_;
  int gcs_port_ = 0;
  ClusterID cluster_id_;
  bool should_fetch_cluster_id_;
};

/// \class GcsClient
/// Abstract interface of the GCS client.
///
/// To read and write from the GCS, `Connect()` must be called and return Status::OK.
/// Before exit, `Disconnect()` must be called.
class RAY_EXPORT GcsClient : public std::enable_shared_from_this<GcsClient> {
 public:
  GcsClient() = default;
  /// Constructor of GcsClient.
  ///
  /// \param options Options for client.
  /// \param gcs_client_id The unique ID for the owner of this object.
  ///    This potentially will be used to tell GCS who is client connecting
  ///    to GCS.
  explicit GcsClient(const GcsClientOptions &options,
                     UniqueID gcs_client_id = UniqueID::FromRandom());

  virtual ~GcsClient() { Disconnect(); };

  /// Connect to GCS Service. Non-thread safe.
  /// This function must be called before calling other functions.
  ///
  /// If cluster_id in options is Nil, sends a blocking RPC to GCS to get the cluster ID.
  /// If returns OK, GetClusterId() will return a non-Nil cluster ID.
  ///
  /// Warning: since it may send *sync* RPCs to GCS, if the caller is in GCS itself, it
  /// must provide a non-Nil cluster ID to avoid deadlocks.
  ///
  /// Thread Safety: GcsClient holds unique ptr to client_call_manager_ which is used
  /// by RPC calls. Before a call to `Connect()` or after a `Disconnect()`, that field
  /// is nullptr and a call to RPC methods can cause segfaults.
  ///
  ///
  /// \param instrumented_io_context IO execution service.
  /// \param timeout_ms Timeout in milliseconds, default to
  /// gcs_rpc_server_connect_timeout_s (5s).
  ///
  /// \return Status
  virtual Status Connect(instrumented_io_context &io_service, int64_t timeout_ms = -1);

  /// Disconnect with GCS Service. Non-thread safe.
  /// Must be called without any concurrent RPC calls. After this call, the client
  /// must not be used until a next Connect() call.
  virtual void Disconnect();

  virtual std::pair<std::string, int> GetGcsServerAddress() const;

  /// Return client information for debug.
  virtual std::string DebugString() const { return ""; }

  /// Resubscribe to GCS to recover from a GCS failure.
  void AsyncResubscribe() {
    if (resubscribe_func_ != nullptr) {
      resubscribe_func_();
    }
  }

  /// Get the sub-interface for accessing actor information in GCS.
  /// This function is thread safe.
  ActorInfoAccessor &Actors() {
    RAY_CHECK(actor_accessor_ != nullptr);
    return *actor_accessor_;
  }

  /// Get the sub-interface for accessing job information in GCS.
  /// This function is thread safe.
  JobInfoAccessor &Jobs() {
    RAY_CHECK(job_accessor_ != nullptr);
    return *job_accessor_;
  }

  /// Get the sub-interface for accessing node information in GCS.
  /// This function is thread safe.
  NodeInfoAccessor &Nodes() {
    RAY_CHECK(node_accessor_ != nullptr);
    return *node_accessor_;
  }

  /// Get the sub-interface for accessing node resource information in GCS.
  /// This function is thread safe.
  NodeResourceInfoAccessor &NodeResources() {
    RAY_CHECK(node_resource_accessor_ != nullptr);
    return *node_resource_accessor_;
  }

  /// Get the sub-interface for accessing error information in GCS.
  /// This function is thread safe.
  ErrorInfoAccessor &Errors() {
    RAY_CHECK(error_accessor_ != nullptr);
    return *error_accessor_;
  }

  TaskInfoAccessor &Tasks() {
    RAY_CHECK(task_accessor_ != nullptr);
    return *task_accessor_;
  }

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  WorkerInfoAccessor &Workers() {
    RAY_CHECK(worker_accessor_ != nullptr);
    return *worker_accessor_;
  }

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  PlacementGroupInfoAccessor &PlacementGroups() {
    RAY_CHECK(placement_group_accessor_ != nullptr);
    return *placement_group_accessor_;
  }

  RuntimeEnvAccessor &RuntimeEnvs() {
    RAY_CHECK(runtime_env_accessor_ != nullptr);
    return *runtime_env_accessor_;
  }

  AutoscalerStateAccessor &Autoscaler() {
    RAY_CHECK(autoscaler_state_accessor_ != nullptr);
    return *autoscaler_state_accessor_;
  }

  // Gets ClusterID. If it's not set in Connect(), blocks on a sync RPC to GCS to get it.
  virtual ClusterID GetClusterId() const;

  /// Get the sub-interface for accessing worker information in GCS.
  /// This function is thread safe.
  virtual InternalKVAccessor &InternalKV() { return *internal_kv_accessor_; }

  virtual GcsSubscriber &GetGcsSubscriber() { return *gcs_subscriber_; }

  virtual rpc::GcsRpcClient &GetGcsRpcClient() { return *gcs_rpc_client_; }

 protected:
  GcsClientOptions options_;

  std::unique_ptr<ActorInfoAccessor> actor_accessor_;
  std::unique_ptr<JobInfoAccessor> job_accessor_;
  std::unique_ptr<NodeInfoAccessor> node_accessor_;
  std::unique_ptr<NodeResourceInfoAccessor> node_resource_accessor_;
  std::unique_ptr<ErrorInfoAccessor> error_accessor_;
  std::unique_ptr<WorkerInfoAccessor> worker_accessor_;
  std::unique_ptr<PlacementGroupInfoAccessor> placement_group_accessor_;
  std::unique_ptr<InternalKVAccessor> internal_kv_accessor_;
  std::unique_ptr<TaskInfoAccessor> task_accessor_;
  std::unique_ptr<RuntimeEnvAccessor> runtime_env_accessor_;
  std::unique_ptr<AutoscalerStateAccessor> autoscaler_state_accessor_;

 private:
  /// If client_call_manager_ does not have a cluster ID, fetches it from GCS. The
  /// fetched cluster ID is set to client_call_manager_.
  Status FetchClusterId(int64_t timeout_ms);

  const UniqueID gcs_client_id_ = UniqueID::FromRandom();

  std::unique_ptr<GcsSubscriber> gcs_subscriber_;

  // Gcs rpc client
  std::shared_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::function<void()> resubscribe_func_;
};

// Connects a GcsClient to the GCS server, on a shared lazy-initialized singleton
// io_context. This is useful for connecting to the GCS server from Python.
//
// For param descriptions, see GcsClient::Connect().
Status ConnectOnSingletonIoContext(GcsClient &gcs_client, int64_t timeout_ms);

// This client is only supposed to be used from Cython / Python
class RAY_EXPORT PythonGcsClient {
 public:
  explicit PythonGcsClient(const GcsClientOptions &options);

  Status Connect(int64_t timeout_ms, size_t num_retries);

  Status CheckAlive(const std::vector<std::string> &raylet_addresses,
                    int64_t timeout_ms,
                    std::vector<bool> &result);

  Status InternalKVGet(const std::string &ns,
                       const std::string &key,
                       int64_t timeout_ms,
                       std::string &value);
  Status InternalKVMultiGet(const std::string &ns,
                            const std::vector<std::string> &keys,
                            int64_t timeout_ms,
                            std::unordered_map<std::string, std::string> &result);
  Status InternalKVPut(const std::string &ns,
                       const std::string &key,
                       const std::string &value,
                       bool overwrite,
                       int64_t timeout_ms,
                       int &added_num);
  Status InternalKVDel(const std::string &ns,
                       const std::string &key,
                       bool del_by_prefix,
                       int64_t timeout_ms,
                       int &deleted_num);
  Status InternalKVKeys(const std::string &ns,
                        const std::string &prefix,
                        int64_t timeout_ms,
                        std::vector<std::string> &results);
  Status InternalKVExists(const std::string &ns,
                          const std::string &key,
                          int64_t timeout_ms,
                          bool &exists);

  Status PinRuntimeEnvUri(const std::string &uri, int expiration_s, int64_t timeout_ms);
  Status GetAllNodeInfo(int64_t timeout_ms, std::vector<rpc::GcsNodeInfo> &result);
  Status GetAllJobInfo(const std::optional<std::string> &job_or_submission_id,
                       bool skip_submission_job_info_field,
                       bool skip_is_running_tasks_field,
                       int64_t timeout_ms,
                       std::vector<rpc::JobTableData> &result);
  Status GetAllResourceUsage(int64_t timeout_ms, std::string &serialized_reply);
  // For rpc::autoscaler::AutoscalerStateService
  Status RequestClusterResourceConstraint(
      int64_t timeout_ms,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const std::vector<int64_t> &count_array);
  Status GetClusterStatus(int64_t timeout_ms, std::string &serialized_reply);
  Status GetClusterResourceState(int64_t timeout_ms, std::string &serialized_reply);
  Status ReportAutoscalingState(int64_t timeout_ms, const std::string &serialized_state);
  Status DrainNode(const std::string &node_id,
                   int32_t reason,
                   const std::string &reason_message,
                   int64_t deadline_timestamp_ms,
                   int64_t timeout_ms,
                   bool &is_accepted,
                   std::string &rejection_reason_message);
  Status DrainNodes(const std::vector<std::string> &node_ids,
                    int64_t timeout_ms,
                    std::vector<std::string> &drained_node_ids);

  const ClusterID &GetClusterId() const { return cluster_id_; }

 private:
  void PrepareContext(grpc::ClientContext &context, int64_t timeout_ms) {
    if (timeout_ms != -1) {
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(timeout_ms));
    }
    if (!cluster_id_.IsNil()) {
      context.AddMetadata(kClusterIdKey, cluster_id_.Hex());
    }
  }

  const GcsClientOptions options_;
  ClusterID cluster_id_;
  std::unique_ptr<rpc::InternalKVGcsService::Stub> kv_stub_;
  std::unique_ptr<rpc::RuntimeEnvGcsService::Stub> runtime_env_stub_;
  std::unique_ptr<rpc::NodeInfoGcsService::Stub> node_info_stub_;
  std::unique_ptr<rpc::NodeResourceInfoGcsService::Stub> node_resource_info_stub_;
  std::unique_ptr<rpc::JobInfoGcsService::Stub> job_info_stub_;
  std::unique_ptr<rpc::autoscaler::AutoscalerStateService::Stub> autoscaler_stub_;
  std::shared_ptr<grpc::Channel> channel_;
  // Make PythonGcsClient thread safe, so add a mutex to protect it.
  absl::Mutex mutex_;
};

std::unordered_map<std::string, double> PythonGetResourcesTotal(
    const rpc::GcsNodeInfo &node_info);

std::unordered_map<std::string, std::string> PythonGetNodeLabels(
    const rpc::GcsNodeInfo &node_info);

}  // namespace gcs

}  // namespace ray
