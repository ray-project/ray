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
#include "ray/gcs/redis_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class GcsClientOptions
/// GCS client's options (configuration items), such as service address, and service
/// password.
class GcsClientOptions {
 public:
  /// Constructor of GcsClientOptions from redis.
  ///
  /// \param ip redis service ip.
  /// \param port redis service port.
  /// \param password redis service password.
  GcsClientOptions(const std::string &redis_ip, int redis_port,
                   const std::string &password, bool enable_sync_conn = true,
                   bool enable_async_conn = true, bool enable_subscribe_conn = true)
      : redis_ip_(redis_ip),
        redis_port_(redis_port),
        password_(password),
        enable_sync_conn_(enable_sync_conn),
        enable_async_conn_(enable_async_conn),
        enable_subscribe_conn_(enable_subscribe_conn) {
    RAY_LOG(DEBUG) << "Connect to gcs server via redis: " << redis_ip << ":"
                   << redis_port;
  }

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

  // redis server address
  std::string redis_ip_;
  int redis_port_ = 0;

  // Password of GCS server.
  std::string password_;

  // Whether to enable connection for contexts.
  bool enable_sync_conn_{true};
  bool enable_async_conn_{true};
  bool enable_subscribe_conn_{true};
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
  /// \param get_gcs_server_address_func Function to get GCS server address.
  explicit GcsClient(const GcsClientOptions &options,
                     std::function<bool(std::pair<std::string, int> *)>
                         get_gcs_server_address_func = nullptr);

  virtual ~GcsClient() = default;

  /// Connect to GCS Service. Non-thread safe.
  /// This function must be called before calling other functions.
  ///
  /// \return Status
  virtual Status Connect(instrumented_io_context &io_service);

  /// Disconnect with GCS Service. Non-thread safe.
  virtual void Disconnect();

  virtual std::pair<std::string, int> GetGcsServerAddress();

  /// Return client information for debug.
  virtual std::string DebugString() const { return ""; }

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

  /// Get the sub-interface for accessing stats information in GCS.
  /// This function is thread safe.
  StatsInfoAccessor &Stats() {
    RAY_CHECK(stats_accessor_ != nullptr);
    return *stats_accessor_;
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

  /// Whether this client is connected to GCS.
  std::atomic<bool> is_connected_{false};

  std::unique_ptr<ActorInfoAccessor> actor_accessor_;
  std::unique_ptr<JobInfoAccessor> job_accessor_;
  std::unique_ptr<NodeInfoAccessor> node_accessor_;
  std::unique_ptr<NodeResourceInfoAccessor> node_resource_accessor_;
  std::unique_ptr<ErrorInfoAccessor> error_accessor_;
  std::unique_ptr<StatsInfoAccessor> stats_accessor_;
  std::unique_ptr<WorkerInfoAccessor> worker_accessor_;
  std::unique_ptr<PlacementGroupInfoAccessor> placement_group_accessor_;
  std::unique_ptr<InternalKVAccessor> internal_kv_accessor_;

 private:
  /// Get gcs server address from redis.
  /// This address is set by GcsServer::StoreGcsServerAddressInRedis function.
  ///
  /// \param context The context of redis.
  /// \param address The address of gcs server.
  /// \param max_attempts The maximum number of times to get gcs server rpc address.
  /// \return Returns true if gcs server address is obtained, False otherwise.
  bool GetGcsServerAddressFromRedis(redisContext *context,
                                    std::pair<std::string, int> *address,
                                    int max_attempts = 1);

  /// Fire a periodic timer to check if GCS sever address has changed.
  void PeriodicallyCheckGcsServerAddress();

  /// This function is used to redo subscription and reconnect to GCS RPC server when gcs
  /// service failure is detected.
  ///
  /// \param type The type of GCS service failure.
  void GcsServiceFailureDetected(rpc::GcsServiceFailureType type);

  /// Reconnect to GCS RPC server.
  void ReconnectGcsServer();

  std::shared_ptr<RedisClient> redis_client_;

  const UniqueID gcs_client_id_ = UniqueID::FromRandom();

  std::unique_ptr<GcsSubscriber> gcs_subscriber_;

  // Gcs rpc client
  std::shared_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::atomic<bool> disconnected_ = false;

  // The runner to run function periodically.
  std::unique_ptr<PeriodicalRunner> periodical_runner_;
  std::function<bool(std::pair<std::string, int> *)> get_server_address_func_;
  std::function<void(bool)> resubscribe_func_;
  std::pair<std::string, int> current_gcs_server_address_;
  int64_t last_reconnect_timestamp_ms_;
  std::pair<std::string, int> last_reconnect_address_;

  /// Retry interval to reconnect to a GCS server.
  const int64_t kGCSReconnectionRetryIntervalMs = 1000;
};

}  // namespace gcs

}  // namespace ray
