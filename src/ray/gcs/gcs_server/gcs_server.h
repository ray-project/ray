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
#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/gcs/gcs_client/usage_stats_client.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_health_check_manager.h"
#include "ray/gcs/gcs_server/gcs_heartbeat_manager.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/gcs/gcs_server/gcs_redis_failure_detector.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/gcs_task_manager.h"
#include "ray/gcs/gcs_server/grpc_based_resource_broadcaster.h"
#include "ray/gcs/gcs_server/pubsub_handler.h"
#include "ray/gcs/gcs_server/ray_syncer.h"
#include "ray/gcs/gcs_server/runtime_env_handler.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_client.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "ray/util/throttler.h"

namespace ray {
using raylet::ClusterTaskManager;
using raylet::NoopLocalTaskManager;
namespace gcs {

struct GcsServerConfig {
  std::string grpc_server_name = "GcsServer";
  uint16_t grpc_server_port = 0;
  uint16_t grpc_server_thread_num = 1;
  std::string redis_password;
  std::string redis_address;
  uint16_t redis_port = 6379;
  bool enable_redis_ssl = false;
  bool retry_redis = true;
  bool enable_sharding_conn = false;
  std::string node_ip_address;
  std::string log_dir;
  // This includes the config list of raylet.
  std::string raylet_config_list;
};

class GcsNodeManager;
class GcsActorManager;
class GcsJobManager;
class GcsWorkerManager;
class GcsPlacementGroupScheduler;
class GcsPlacementGroupManager;
class GcsTaskManager;

/// The GcsServer will take over all requests from GcsClient and transparent
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
  /// Generate the redis client options
  RedisClientOptions GetRedisClientOptions() const;

  void DoStart(const GcsInitData &gcs_init_data);

  /// Initialize gcs node manager.
  void InitGcsNodeManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs heartbeat manager.
  void InitGcsHeartbeatManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs resource manager.
  void InitGcsResourceManager(const GcsInitData &gcs_init_data);

  /// Initialize synchronization service
  void InitRaySyncer(const GcsInitData &gcs_init_data);

  /// Initialize cluster resource scheduler.
  void InitClusterResourceScheduler();

  /// Initialize cluster task manager.
  void InitClusterTaskManager();

  /// Initialize gcs job manager.
  void InitGcsJobManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs actor manager.
  void InitGcsActorManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs placement group manager.
  void InitGcsPlacementGroupManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs worker manager.
  void InitGcsWorkerManager();

  /// Initialize gcs task manager.
  void InitGcsTaskManager();

  /// Initialize stats handler.
  void InitStatsHandler();

  /// Initialize usage stats client.
  void InitUsageStatsClient();

  /// Initialize KV manager.
  void InitKVManager();

  /// Initialize function manager.
  void InitFunctionManager();

  /// Initializes PubSub handler.
  void InitPubSubHandler();

  // Init RuntimeENv manager
  void InitRuntimeEnvManager();

  /// Install event listeners.
  void InstallEventListeners();

 private:
  /// Gets the type of KV storage to use from config.
  std::string StorageType() const;

  /// Print debug info periodically.
  std::string GetDebugState() const;

  /// Dump the debug info to debug_state_gcs.txt.
  void DumpDebugStateToFile() const;

  /// Collect stats from each module.
  void RecordMetrics() const;

  /// Print the asio event loop stats for debugging.
  void PrintAsioStats();

  /// Get or connect to a redis server
  std::shared_ptr<RedisClient> GetOrConnectRedis();

  void TryGlobalGC();

  /// Gcs server configuration.
  const GcsServerConfig config_;
  // Type of storage to use.
  const std::string storage_type_;
  /// The main io service to drive event posted from grpc threads.
  instrumented_io_context &main_service_;
  /// The io service used by heartbeat manager in case of node failure detector being
  /// blocked by main thread.
  instrumented_io_context heartbeat_manager_io_service_;
  /// The io service used by Pubsub, for isolation from other workload.
  instrumented_io_context pubsub_io_service_;
  /// The grpc server
  rpc::GrpcServer rpc_server_;
  /// The `ClientCallManager` object that is shared by all `NodeManagerWorkerClient`s.
  rpc::ClientCallManager client_call_manager_;
  /// Node manager client pool.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// The gcs resource manager.
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  /// The gcs server's node id, for the creation of `cluster_resource_scheduler_` and
  /// `cluster_task_manager_`.
  NodeID local_node_id_;
  /// The cluster resource scheduler.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// The cluster task manager.
  std::shared_ptr<ClusterTaskManager> cluster_task_manager_;
  /// The gcs node manager.
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
  /// The health check manager.
  std::shared_ptr<GcsHealthCheckManager> gcs_healthcheck_manager_;
  /// The heartbeat manager.
  std::shared_ptr<GcsHeartbeatManager> gcs_heartbeat_manager_;
  /// The gcs redis failure detector.
  std::shared_ptr<GcsRedisFailureDetector> gcs_redis_failure_detector_;
  /// The gcs actor manager.
  std::shared_ptr<GcsActorManager> gcs_actor_manager_;
  /// The gcs placement group scheduler.
  std::shared_ptr<GcsPlacementGroupScheduler> gcs_placement_group_scheduler_;
  /// The gcs placement group manager.
  std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  /// Job info handler and service.
  std::unique_ptr<GcsJobManager> gcs_job_manager_;
  std::unique_ptr<rpc::JobInfoGrpcService> job_info_service_;
  /// Actor info service.
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  /// Node info handler and service.
  std::unique_ptr<rpc::NodeInfoGrpcService> node_info_service_;
  /// Function table manager.
  std::unique_ptr<GcsFunctionManager> function_manager_;
  /// Node resource info handler and service.
  std::unique_ptr<rpc::NodeResourceInfoGrpcService> node_resource_info_service_;
  /// Heartbeat info handler and service.
  std::unique_ptr<rpc::HeartbeatInfoGrpcService> heartbeat_info_service_;
  /// Stats handler and service.
  std::unique_ptr<rpc::StatsHandler> stats_handler_;
  std::unique_ptr<rpc::StatsGrpcService> stats_service_;

  /// Synchronization service for ray.
  /// TODO(iycheng): Deprecate this gcs_ray_syncer_ one once we roll out
  /// to ray_syncer_.
  std::unique_ptr<gcs_syncer::RaySyncer> gcs_ray_syncer_;

  /// Ray Syncer realted fields.
  std::unique_ptr<syncer::RaySyncer> ray_syncer_;
  std::unique_ptr<std::thread> ray_syncer_thread_;
  instrumented_io_context ray_syncer_io_context_;

  /// The node id of GCS.
  NodeID gcs_node_id_;

  /// The usage stats client.
  std::unique_ptr<UsageStatsClient> usage_stats_client_;
  /// The gcs worker manager.
  std::unique_ptr<GcsWorkerManager> gcs_worker_manager_;
  /// Worker info service.
  std::unique_ptr<rpc::WorkerInfoGrpcService> worker_info_service_;
  /// Placement Group info handler and service.
  std::unique_ptr<rpc::PlacementGroupInfoGrpcService> placement_group_info_service_;
  /// Global KV storage handler and service.
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  std::unique_ptr<rpc::InternalKVGrpcService> kv_service_;
  /// Runtime env handler and service.
  std::unique_ptr<RuntimeEnvHandler> runtime_env_handler_;
  std::unique_ptr<rpc::RuntimeEnvGrpcService> runtime_env_service_;
  /// GCS PubSub handler and service.
  std::unique_ptr<InternalPubSubHandler> pubsub_handler_;
  std::unique_ptr<rpc::InternalPubSubGrpcService> pubsub_service_;
  /// GCS Task info manager for managing task states change events.
  std::unique_ptr<GcsTaskManager> gcs_task_manager_;
  /// Independent task info service from the main grpc service.
  std::unique_ptr<rpc::TaskInfoGrpcService> task_info_service_;
  /// Backend client.
  std::shared_ptr<RedisClient> redis_client_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<GcsPublisher> gcs_publisher_;
  /// Grpc based pubsub's periodical runner.
  PeriodicalRunner pubsub_periodical_runner_;
  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;
  /// The gcs table storage.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Stores references to URIs stored by the GCS for runtime envs.
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_manager_;
  /// Gcs service state flag, which is used for ut.
  std::atomic<bool> is_started_;
  std::atomic<bool> is_stopped_;
  int task_pending_schedule_detected_ = 0;
  /// Throttler for global gc
  std::unique_ptr<Throttler> global_gc_throttler_;
};

}  // namespace gcs
}  // namespace ray
