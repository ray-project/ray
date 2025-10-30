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

#include <memory>
#include <string>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/postable.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_function_manager.h"
#include "ray/gcs/gcs_health_check_manager.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/gcs_server_io_context_policy.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/gcs_task_manager.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs/pubsub_handler.h"
#include "ray/gcs/runtime_env_handler.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/observability/metric_interface.h"
#include "ray/observability/ray_event_recorder.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/metrics_agent_client.h"
#include "ray/util/throttler.h"

namespace ray {
using raylet::ClusterLeaseManager;
using raylet::NoopLocalLeaseManager;

namespace rpc {
class ClientCallManager;
}

namespace gcs {

struct GcsServerConfig {
  std::string grpc_server_name = "GcsServer";
  uint16_t grpc_server_port = 0;
  uint16_t grpc_server_thread_num = 1;
  uint16_t metrics_agent_port = 0;
  std::string redis_username;
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
  std::string session_name;
};

class GcsNodeManager;
class GcsActorManager;
class GcsJobManager;
class GcsWorkerManager;
class GcsPlacementGroupScheduler;
class GcsPlacementGroupManager;
class GcsTaskManager;
class GcsAutoscalerStateManager;
struct RedisClientOptions;

/// The GcsServer will take over all requests from GcsClient and transparent
/// transmit the command to the backend reliable storage for the time being.
/// In the future, GCS server's main responsibility is to manage meta data
/// and the management of actor creation.
/// For more details, please see the design document.
/// https://docs.google.com/document/d/1d-9qBlsh2UQHo-AWMWR0GptI_Ajwu4SKx0Q0LHKPpeI/edit#heading=h.csi0gaglj2pv
///
/// Notes on lifecycle:
/// 1. Gcs server contains a lot of data member, gcs server outlives all of them.
/// 2. Gcs table storage and all gcs managers share a lifetime, that starts from a
/// `DoStart` call to `Stop`.
class GcsServer {
 public:
  GcsServer(const GcsServerConfig &config,
            const ray::gcs::GcsServerMetrics &metrics,
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

  /// Retrieve cluster ID
  const ClusterID &GetClusterId() const { return rpc_server_.GetClusterId(); }

  // TODO(vitsai): string <=> enum generator macro
  enum class StorageType {
    UNKNOWN = 0,
    IN_MEMORY = 1,
    REDIS_PERSIST = 2,
  };

  static constexpr char kInMemoryStorage[] = "memory";
  static constexpr char kRedisStorage[] = "redis";

  void UpdateGcsResourceManagerInTest(
      const NodeID &node_id,
      const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
    RAY_CHECK(gcs_resource_manager_ != nullptr);
    gcs_resource_manager_->UpdateFromResourceView(node_id, resource_view_sync_message);
  }

 protected:
  void DoStart(const GcsInitData &gcs_init_data);

  /// Initialize gcs node manager.
  void InitGcsNodeManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs health check manager.
  void InitGcsHealthCheckManager(const GcsInitData &gcs_init_data);

  /// Initialize gcs resource manager.
  void InitGcsResourceManager(const GcsInitData &gcs_init_data);

  /// Initialize synchronization service
  void InitRaySyncer(const GcsInitData &gcs_init_data);

  /// Initialize cluster resource scheduler.
  void InitClusterResourceScheduler();

  /// Initialize cluster lease manager.
  void InitClusterLeaseManager();

  /// Initialize gcs job manager.
  void InitGcsJobManager(
      const GcsInitData &gcs_init_data,
      ray::observability::MetricInterface &running_job_gauge,
      ray::observability::MetricInterface &finished_job_counter,
      ray::observability::MetricInterface &job_duration_in_seconds_gauge);

  /// Initialize gcs actor manager.
  void InitGcsActorManager(const GcsInitData &gcs_init_data,
                           ray::observability::MetricInterface &actor_by_state_gauge,
                           ray::observability::MetricInterface &gcs_actor_by_state_gauge);

  /// Initialize gcs placement group manager.
  void InitGcsPlacementGroupManager(
      const GcsInitData &gcs_init_data,
      ray::observability::MetricInterface &placement_group_gauge,
      ray::observability::MetricInterface
          &placement_group_creation_latency_in_ms_histogram,
      ray::observability::MetricInterface
          &placement_group_scheduling_latency_in_ms_histogram,
      ray::observability::MetricInterface &placement_group_count_gauge);

  /// Initialize gcs worker manager.
  void InitGcsWorkerManager();

  /// Initialize gcs task manager.
  void InitGcsTaskManager(ray::observability::MetricInterface &task_events_reported_gauge,
                          ray::observability::MetricInterface &task_events_dropped_gauge,
                          ray::observability::MetricInterface &task_events_stored_gauge);

  /// Initialize gcs autoscaling manager.
  void InitGcsAutoscalerStateManager(const GcsInitData &gcs_init_data);

  /// Initialize usage stats client.
  void InitUsageStatsClient();

  /// Initialize KV manager.
  void InitKVManager();

  /// Initialize KV service.
  void InitKVService();

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
  StorageType GetStorageType() const;

  /// Print debug info periodically.
  void PrintDebugState() const;

  /// Collect stats from each module.
  void RecordMetrics() const;

  /// Get cluster id if persisted, otherwise generate
  /// a new one and persist as necessary.
  /// Expected to be idempotent while server is up.
  /// Makes several InternalKV calls, all in continuation.io_context().
  void GetOrGenerateClusterId(Postable<void(ClusterID cluster_id)> continuation);

  RedisClientOptions GetRedisClientOptions();

  void TryGlobalGC();

  /// GCS server metrics
  const ray::gcs::GcsServerMetrics &metrics_;
  IOContextProvider<GcsServerIOContextPolicy> io_context_provider_;

  /// NOTICE: The declaration order for data members should follow dependency.
  ///
  /// Gcs server configuration.
  const GcsServerConfig config_;
  // Type of storage to use.
  const StorageType storage_type_;
  /// The grpc server
  rpc::GrpcServer rpc_server_;
  /// The `ClientCallManager` object that is shared by all `RayletClient`s.
  rpc::ClientCallManager client_call_manager_;
  /// Node manager client pool.
  rpc::RayletClientPool raylet_client_pool_;
  // Core worker client pool.
  rpc::CoreWorkerClientPool worker_client_pool_;
  /// The cluster resource scheduler.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// Local lease manager.
  NoopLocalLeaseManager local_lease_manager_;
  /// The gcs table storage.
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// The cluster lease manager.
  std::unique_ptr<ClusterLeaseManager> cluster_lease_manager_;
  /// [gcs_resource_manager_] depends on [cluster_lease_manager_].
  /// The gcs resource manager.
  std::unique_ptr<GcsResourceManager> gcs_resource_manager_;
  /// The autoscaler state manager.
  std::unique_ptr<GcsAutoscalerStateManager> gcs_autoscaler_state_manager_;
  /// A publisher for publishing gcs messages.
  std::unique_ptr<pubsub::GcsPublisher> gcs_publisher_;
  /// The gcs node manager.
  std::unique_ptr<GcsNodeManager> gcs_node_manager_;
  /// The health check manager.
  std::shared_ptr<GcsHealthCheckManager> gcs_healthcheck_manager_;
  /// The gcs placement group manager.
  std::unique_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  /// The gcs actor manager.
  std::unique_ptr<GcsActorManager> gcs_actor_manager_;
  /// The gcs placement group scheduler.
  /// [gcs_placement_group_scheduler_] depends on [raylet_client_pool_].
  std::unique_ptr<GcsPlacementGroupScheduler> gcs_placement_group_scheduler_;
  /// Function table manager.
  std::unique_ptr<GCSFunctionManager> function_manager_;
  /// Stores references to URIs stored by the GCS for runtime envs.
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_manager_;
  /// Global KV storage handler.
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  /// Job info handler.
  std::unique_ptr<GcsJobManager> gcs_job_manager_;
  /// The Ray event recorder that is used to record events (e.g. job events, node events,
  /// etc.).
  rpc::ClientCallManager event_aggregator_client_call_manager_;
  std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client_;
  std::unique_ptr<observability::RayEventRecorder> ray_event_recorder_;

  /// Ray Syncer related fields.
  std::unique_ptr<syncer::RaySyncer> ray_syncer_;
  std::unique_ptr<syncer::RaySyncerService> ray_syncer_service_;

  /// The node id of GCS.
  NodeID gcs_node_id_;

  /// The usage stats client.
  std::unique_ptr<UsageStatsClient> usage_stats_client_;
  /// The gcs worker manager.
  std::unique_ptr<GcsWorkerManager> gcs_worker_manager_;
  /// Runtime env handler.
  std::unique_ptr<RuntimeEnvHandler> runtime_env_handler_;
  /// GCS PubSub handler.
  std::unique_ptr<InternalPubSubHandler> pubsub_handler_;
  /// GCS Task info manager for managing task states change events.
  std::unique_ptr<GcsTaskManager> gcs_task_manager_;
  /// Grpc based pubsub's periodical runner.
  std::shared_ptr<PeriodicalRunner> pubsub_periodical_runner_;
  /// The runner to run function periodically.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  /// Gcs service state flag, which is used for ut.
  std::atomic<bool> is_started_;
  std::atomic<bool> is_stopped_;
  int task_pending_schedule_detected_ = 0;
  /// Throttler for global gc
  std::unique_ptr<Throttler> global_gc_throttler_;
  /// Client to call a metrics agent gRPC server.
  std::unique_ptr<rpc::MetricsAgentClient> metrics_agent_client_;
};

}  // namespace gcs
}  // namespace ray
