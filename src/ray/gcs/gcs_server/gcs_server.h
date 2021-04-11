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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_heartbeat_manager.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/gcs/gcs_server/gcs_object_manager.h"
#include "ray/gcs/gcs_server/gcs_redis_failure_detector.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_client.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

struct GcsServerConfig {
  std::string grpc_server_name = "GcsServer";
  uint16_t grpc_server_port = 0;
  uint16_t grpc_server_thread_num = 1;
  std::string redis_password;
  std::string redis_address;
  uint16_t redis_port = 6379;
  bool retry_redis = true;
  bool enable_sharding_conn = true;
  std::string node_ip_address;
  bool pull_based_resource_reporting;
};

class GcsNodeManager;
class GcsActorManager;
class GcsJobManager;
class GcsWorkerManager;
class GcsPlacementGroupManager;

/// The GcsServer will take over all requests from ServiceBasedGcsClient and transparent
/// transmit the command to the backend reliable storage for the time being.
/// In the future, GCS server's main responsibility is to manage meta data
/// and the management of actor creation.
/// For more details, please see the design document.
/// https://docs.google.com/document/d/1d-9qBlsh2UQHo-AWMWR0GptI_Ajwu4SKx0Q0LHKPpeI/edit#heading=h.csi0gaglj2pv
class GcsServer {
 public:
  explicit GcsServer(const GcsServerConfig &config,
                     instrumented_io_context &main_service);
  virtual ~GcsServer();

  /// Start gcs server.
  void Start();

  /// Stop gcs server.
  void Stop();

  /// Get the port of this gcs server.
  int GetPort() const { return rpc_server_.GetPort(); }

  /// Check if gcs server is started.
  bool IsStarted() const { return is_started_; }

  /// Check if gcs server is stopped.
  bool IsStopped() const { return is_stopped_; }

 protected:
  void DoStart(const GcsInitData &gcs_init_data);

  /// Initialize gcs node manager.
  void InitGcsNodeManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs heartbeat manager.
  void InitGcsHeartbeatManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs resource manager.
  void InitGcsResourceManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs resource scheduler.
  void InitGcsResourceScheduler();

  /// Initialize gcs job manager.
  void InitGcsJobManager();

  /// Initialize gcs actor manager.
  void InitGcsActorManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs placement group manager.
  void InitGcsPlacementGroupManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs object manager.
  void InitObjectManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs worker manager.
  void InitGcsWorkerManager();

  /// Initialize task info handler.
  void InitTaskInfoHandler();

  /// Initialize stats handler.
  void InitStatsHandler();

  /// Initialize KV manager.
  void InitKVManager();

  /// Initialize resource report polling.
  void InitResourceReportPolling(const GcsInitData &gcs_init_data);

  /// Install event listeners.
  void InstallEventListeners();

 private:
  /// Store the address of GCS server in Redis.
  ///
  /// Clients will look up this address in Redis and use it to connect to GCS server.
  /// TODO(ffbin): Once we entirely migrate to service-based GCS, we should pass GCS
  /// server address directly to raylets and get rid of this lookup.
  void StoreGcsServerAddressInRedis();

  /// Collect stats from each module for every (metrics_report_interval_ms / 2) ms.
  void CollectStats();

  /// Print debug info periodically.
  void PrintDebugInfo();

  /// Print the asio event loop stats for debugging.
  void PrintAsioStats();

  /// Gcs server configuration.
  GcsServerConfig config_;
  /// The main io service to drive event posted from grpc threads.
  instrumented_io_context &main_service_;
  /// The io service used by heartbeat manager in case of node failure detector being
  /// blocked by main thread.
  instrumented_io_context heartbeat_manager_io_service_;
  /// The grpc server
  rpc::GrpcServer rpc_server_;
  /// The `ClientCallManager` object that is shared by all `NodeManagerWorkerClient`s.
  rpc::ClientCallManager client_call_manager_;
  /// Node manager client pool.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// The gcs resource manager.
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  /// The gcs resource scheduler.
  std::shared_ptr<GcsResourceScheduler> gcs_resource_scheduler_;
  /// The gcs node manager.
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
  /// The heartbeat manager.
  std::shared_ptr<GcsHeartbeatManager> gcs_heartbeat_manager_;
  /// The gcs redis failure detector.
  std::shared_ptr<GcsRedisFailureDetector> gcs_redis_failure_detector_;
  /// The gcs actor manager.
  std::shared_ptr<GcsActorManager> gcs_actor_manager_;
  /// The gcs placement group manager.
  std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  /// Job info handler and service.
  std::unique_ptr<GcsJobManager> gcs_job_manager_;
  std::unique_ptr<rpc::JobInfoGrpcService> job_info_service_;
  /// Actor info service.
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  /// Node info handler and service.
  std::unique_ptr<rpc::NodeInfoGrpcService> node_info_service_;
  /// Node resource info handler and service.
  std::unique_ptr<rpc::NodeResourceInfoGrpcService> node_resource_info_service_;
  /// Heartbeat info handler and service.
  std::unique_ptr<rpc::HeartbeatInfoGrpcService> heartbeat_info_service_;
  /// Object info handler and service.
  std::unique_ptr<gcs::GcsObjectManager> gcs_object_manager_;
  std::unique_ptr<rpc::ObjectInfoGrpcService> object_info_service_;
  /// Task info handler and service.
  std::unique_ptr<rpc::TaskInfoHandler> task_info_handler_;
  std::unique_ptr<rpc::TaskInfoGrpcService> task_info_service_;
  /// Stats handler and service.
  std::unique_ptr<rpc::StatsHandler> stats_handler_;
  std::unique_ptr<rpc::StatsGrpcService> stats_service_;
  /// Resource report poller.
  std::unique_ptr<GcsResourceReportPoller> gcs_resource_report_poller_;
  /// The gcs worker manager.
  std::unique_ptr<GcsWorkerManager> gcs_worker_manager_;
  /// Worker info service.
  std::unique_ptr<rpc::WorkerInfoGrpcService> worker_info_service_;
  /// Placement Group info handler and service.
  std::unique_ptr<rpc::PlacementGroupInfoGrpcService> placement_group_info_service_;
  /// Global KV storage handler and service.
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  std::unique_ptr<rpc::InternalKVGrpcService> kv_service_;
  /// Backend client.
  std::shared_ptr<RedisClient> redis_client_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// The gcs table storage.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Gcs service state flag, which is used for ut.
  bool is_started_ = false;
  bool is_stopped_ = false;
};

}  // namespace gcs
}  // namespace ray
