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

namespace ray {
namespace gcs {

/// \class GcsClientOptions
/// GCS client's options (configuration items), such as service address, and service
/// password.
class GcsClientOptions {
 public:
  /// Constructor of GcsClientOptions from gcs address
  ///
  /// \param gcs_address gcs address, including port
  GcsClientOptions(const std::string &gcs_address) {
    std::vector<std::string> address = absl::StrSplit(gcs_address, ':');
    RAY_LOG(DEBUG) << "Connect to gcs server via address: " << gcs_address;
    RAY_CHECK(address.size() == 2);
    gcs_address_ = address[0];
    gcs_port_ = std::stoi(address[1]);
  }

  GcsClientOptions() {}

  // Gcs address
  std::string gcs_address_;
  int gcs_port_ = 0;
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
  /// \return Status
  virtual Status Connect(instrumented_io_context &io_service);

  /// Disconnect with GCS Service. Non-thread safe.
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

 private:
  const UniqueID gcs_client_id_ = UniqueID::FromRandom();

  std::unique_ptr<GcsSubscriber> gcs_subscriber_;

  // Gcs rpc client
  std::shared_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::function<void()> resubscribe_func_;
};

// This client is only supposed to be used from Cython / Python
class RAY_EXPORT PythonGcsClient {
 public:
  explicit PythonGcsClient(const GcsClientOptions &options);
  Status Connect();

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
  Status GetAllJobInfo(int64_t timeout_ms, std::vector<rpc::JobTableData> &result);

 private:
  GcsClientOptions options_;
  std::unique_ptr<rpc::InternalKVGcsService::Stub> kv_stub_;
  std::unique_ptr<rpc::RuntimeEnvGcsService::Stub> runtime_env_stub_;
  std::unique_ptr<rpc::NodeInfoGcsService::Stub> node_info_stub_;
  std::unique_ptr<rpc::JobInfoGcsService::Stub> job_info_stub_;
  std::shared_ptr<grpc::Channel> channel_;
};

std::unordered_map<std::string, double> PythonGetResourcesTotal(
    const rpc::GcsNodeInfo &node_info);

}  // namespace gcs

}  // namespace ray
