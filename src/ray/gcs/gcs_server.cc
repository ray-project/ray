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

#include "ray/gcs/gcs_server.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker_rpc_client/core_worker_client.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_actor_manager.h"
#include "ray/gcs/gcs_autoscaler_state_manager.h"
#include "ray/gcs/gcs_job_manager.h"
#include "ray/gcs/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/gcs_worker_manager.h"
#include "ray/gcs/grpc_services.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/observable_store_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/gcs/store_client_kv.h"
#include "ray/observability/metric_constants.h"
#include "ray/pubsub/publisher.h"
#include "ray/raylet_rpc_client/raylet_client.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/stats/stats.h"
#include "ray/util/network_util.h"

namespace ray {
namespace gcs {

inline std::ostream &operator<<(std::ostream &str, GcsServer::StorageType val) {
  switch (val) {
  case GcsServer::StorageType::IN_MEMORY:
    return str << "StorageType::IN_MEMORY";
  case GcsServer::StorageType::REDIS_PERSIST:
    return str << "StorageType::REDIS_PERSIST";
  case GcsServer::StorageType::UNKNOWN:
    return str << "StorageType::UNKNOWN";
  default:
    UNREACHABLE;
  }
}

GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config,
                     const ray::gcs::GcsServerMetrics &metrics,
                     instrumented_io_context &main_service)
    : metrics_(metrics),
      io_context_provider_(main_service),
      config_(config),
      storage_type_(GetStorageType()),
      rpc_server_(config.grpc_server_name,
                  config.grpc_server_port,
                  config.node_ip_address == "127.0.0.1",
                  config.grpc_server_thread_num,
                  /*keepalive_time_ms=*/RayConfig::instance().grpc_keepalive_time_ms()),
      client_call_manager_(main_service,
                           /*record_stats=*/true,
                           config.node_ip_address,
                           ClusterID::Nil(),
                           RayConfig::instance().gcs_server_rpc_client_thread_num()),
      raylet_client_pool_([this](const rpc::Address &addr) {
        return std::make_shared<ray::rpc::RayletClient>(
            addr,
            this->client_call_manager_,
            /*raylet_unavailable_timeout_callback=*/
            [this, addr]() {
              const NodeID node_id = NodeID::FromBinary(addr.node_id());
              auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
              if (!alive_node.has_value()) {
                this->raylet_client_pool_.Disconnect(node_id);
              }
            });
      }),
      worker_client_pool_([this](const rpc::Address &addr) {
        return std::make_shared<rpc::CoreWorkerClient>(
            addr,
            this->client_call_manager_,
            /*core_worker_unavailable_timeout_callback*/ [this, addr]() {
              const NodeID node_id = NodeID::FromBinary(addr.node_id());
              const WorkerID worker_id = WorkerID::FromBinary(addr.worker_id());
              auto alive_node = this->gcs_node_manager_->GetAliveNode(node_id);
              if (!alive_node.has_value()) {
                this->worker_client_pool_.Disconnect(worker_id);
                return;
              }
              auto &node_info = alive_node.value();
              auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
                  node_id,
                  node_info->node_manager_address(),
                  node_info->node_manager_port());
              auto raylet_client =
                  this->raylet_client_pool_.GetOrConnectByAddress(remote_address);
              // Worker could still be dead even if node is alive.
              raylet_client->IsLocalWorkerDead(
                  worker_id,
                  [this, worker_id, node_id](const Status &status, const auto &reply) {
                    if (!status.ok()) {
                      RAY_LOG(INFO).WithField(worker_id).WithField(node_id)
                          << "Failed to check if worker is dead on request to raylet";
                      return;
                    }
                    if (reply.is_dead()) {
                      RAY_LOG(INFO).WithField(worker_id)
                          << "Disconnect core worker client since it is dead";
                      this->worker_client_pool_.Disconnect(worker_id);
                    }
                  });
            });
      }),
      event_aggregator_client_call_manager_(
          io_context_provider_.GetIOContext<observability::RayEventRecorder>(),
          /*record_stats=*/true,
          config.node_ip_address,
          ClusterID::Nil(),
          RayConfig::instance().gcs_server_rpc_client_thread_num()),
      event_aggregator_client_(std::make_unique<rpc::EventAggregatorClientImpl>(
          config_.metrics_agent_port, event_aggregator_client_call_manager_)),
      ray_event_recorder_(std::make_unique<observability::RayEventRecorder>(
          *event_aggregator_client_,
          io_context_provider_.GetIOContext<observability::RayEventRecorder>(),
          RayConfig::instance().ray_event_recorder_max_queued_events(),
          observability::kMetricSourceGCS,
          metrics_.event_recorder_dropped_events_counter)),
      pubsub_periodical_runner_(PeriodicalRunner::Create(
          io_context_provider_.GetIOContext<pubsub::GcsPublisher>())),
      periodical_runner_(
          PeriodicalRunner::Create(io_context_provider_.GetDefaultIOContext())),
      is_started_(false),
      is_stopped_(false) {
  // Init GCS table storage. Note this is on the default io context, not the one with
  // GcsInternalKVManager, to avoid congestion on the latter.
  RAY_LOG(INFO) << "GCS storage type is " << storage_type_;
  auto &io_context = io_context_provider_.GetDefaultIOContext();
  std::shared_ptr<StoreClient> store_client;
  switch (storage_type_) {
  case StorageType::IN_MEMORY:
    store_client = std::make_shared<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(),
        metrics_.storage_operation_latency_in_ms_histogram,
        metrics_.storage_operation_count_counter);
    break;
  case StorageType::REDIS_PERSIST: {
    auto redis_store_client =
        std::make_shared<RedisStoreClient>(io_context, GetRedisClientOptions());
    // Health check Redis periodically and crash if it becomes unavailable.
    // NOTE: periodical_runner_ must run on the same IO context as the Redis client.
    periodical_runner_->RunFnPeriodically(
        [redis_store_client, &io_context] {
          redis_store_client->AsyncCheckHealth(
              {[](const Status &status) {
                 RAY_CHECK_OK(status) << "Redis connection failed unexpectedly.";
               },
               io_context});
        },
        RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds(),
        "GCSServer.redis_health_check");

    store_client = redis_store_client;
    break;
  }
  default:
    RAY_LOG(FATAL) << "Unexpected storage type: " << storage_type_;
  }

  gcs_table_storage_ = std::make_unique<GcsTableStorage>(std::move(store_client));

  auto inner_publisher = std::make_unique<pubsub::Publisher>(
      /*channels=*/
      std::vector<rpc::ChannelType>{
          rpc::ChannelType::GCS_ACTOR_CHANNEL,
          rpc::ChannelType::GCS_JOB_CHANNEL,
          rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
          rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
          rpc::ChannelType::RAY_ERROR_INFO_CHANNEL,
          rpc::ChannelType::RAY_LOG_CHANNEL,
          rpc::ChannelType::RAY_NODE_RESOURCE_USAGE_CHANNEL,
          rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL},
      /*periodical_runner=*/*pubsub_periodical_runner_,
      /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
      /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
      /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
      /*publisher_id=*/NodeID::FromRandom());

  gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(std::move(inner_publisher));
  metrics_agent_client_ = std::make_unique<rpc::MetricsAgentClientImpl>(
      "127.0.0.1",
      config_.metrics_agent_port,
      io_context_provider_.GetDefaultIOContext(),
      client_call_manager_);
}

GcsServer::~GcsServer() { Stop(); }

void GcsServer::Start() {
  // Load gcs tables data asynchronously.
  auto gcs_init_data = std::make_shared<GcsInitData>(*gcs_table_storage_);
  // Init KV Manager. This needs to be initialized first here so that
  // it can be used to retrieve the cluster ID.
  InitKVManager();
  gcs_init_data->AsyncLoad({[this, gcs_init_data] {
                              GetOrGenerateClusterId(
                                  {[this, gcs_init_data](ClusterID cluster_id) {
                                     rpc_server_.SetClusterId(cluster_id);
                                     DoStart(*gcs_init_data);
                                   },
                                   io_context_provider_.GetDefaultIOContext()});
                            },
                            io_context_provider_.GetDefaultIOContext()});
}

void GcsServer::GetOrGenerateClusterId(
    Postable<void(ClusterID cluster_id)> continuation) {
  instrumented_io_context &io_context = continuation.io_context();
  static std::string const kClusterIdNamespace = "cluster";
  kv_manager_->GetInstance().Get(
      kClusterIdNamespace,
      kClusterIdKey,
      {[this, continuation = std::move(continuation)](
           std::optional<std::string> provided_cluster_id) mutable {
         if (!provided_cluster_id.has_value()) {
           instrumented_io_context &io_ctx = continuation.io_context();
           ClusterID cluster_id = ClusterID::FromRandom();
           RAY_LOG(INFO).WithField(cluster_id) << "Generated new cluster ID.";
           kv_manager_->GetInstance().Put(
               kClusterIdNamespace,
               kClusterIdKey,
               cluster_id.Binary(),
               false,
               {[cluster_id,
                 continuation = std::move(continuation)](bool added_entry) mutable {
                  RAY_CHECK(added_entry) << "Failed to persist new cluster ID.";
                  std::move(continuation)
                      .Dispatch("GcsServer.GetOrGenerateClusterId.continuation",
                                cluster_id);
                },
                io_ctx});
         } else {
           ClusterID cluster_id = ClusterID::FromBinary(provided_cluster_id.value());
           RAY_LOG(INFO).WithField(cluster_id)
               << "Using existing cluster ID from external storage.";
           std::move(continuation)
               .Dispatch("GcsServer.GetOrGenerateClusterId.continuation", cluster_id);
         }
       },
       io_context});
}

void GcsServer::DoStart(const GcsInitData &gcs_init_data) {
  InitClusterResourceScheduler();
  InitGcsNodeManager(gcs_init_data);
  InitClusterLeaseManager();
  InitGcsResourceManager(gcs_init_data);
  InitGcsHealthCheckManager(gcs_init_data);
  InitRaySyncer(gcs_init_data);
  InitKVService();
  InitFunctionManager();
  InitPubSubHandler();
  InitRuntimeEnvManager();
  InitGcsJobManager(gcs_init_data,
                    metrics_.running_job_gauge,
                    metrics_.finished_job_counter,
                    metrics_.job_duration_in_seconds_gauge);
  InitGcsPlacementGroupManager(
      gcs_init_data,
      metrics_.placement_group_gauge,
      metrics_.placement_group_creation_latency_in_ms_histogram,
      metrics_.placement_group_scheduling_latency_in_ms_histogram,
      metrics_.placement_group_count_gauge);
  InitGcsActorManager(
      gcs_init_data, metrics_.actor_by_state_gauge, metrics_.gcs_actor_by_state_gauge);
  InitGcsWorkerManager();
  InitGcsTaskManager(metrics_.task_events_reported_gauge,
                     metrics_.task_events_dropped_gauge,
                     metrics_.task_events_stored_gauge);
  InstallEventListeners();
  InitGcsAutoscalerStateManager(gcs_init_data);
  InitUsageStatsClient();

  // Init metrics and event exporter.
  metrics_agent_client_->WaitForServerReady([this](const Status &server_status) {
    if (server_status.ok()) {
      stats::InitOpenTelemetryExporter(config_.metrics_agent_port);
      ray_event_recorder_->StartExportingEvents();
    } else {
      RAY_LOG(ERROR)
          << "Failed to establish connection to the event+metrics exporter agent. "
             "Events and metrics will not be exported. "
          << "Exporter agent status: " << server_status.ToString();
    }
  });

  // Start RPC server when all tables have finished loading initial
  // data.
  rpc_server_.Run();

  periodical_runner_->RunFnPeriodically(
      [this] { RecordMetrics(); },
      /*ms*/ RayConfig::instance().metrics_report_interval_ms() / 2,
      "GCSServer.deadline_timer.metrics_report");

  periodical_runner_->RunFnPeriodically(
      [this] { PrintDebugState(); },
      /*ms*/ RayConfig::instance().event_stats_print_interval_ms(),
      "GCSServer.deadline_timer.debug_state_event_stats_print");

  global_gc_throttler_ =
      std::make_unique<Throttler>(RayConfig::instance().global_gc_min_interval_s() * 1e9);

  periodical_runner_->RunFnPeriodically(
      [this] { TryGlobalGC(); },
      /*ms*/ RayConfig::instance().gcs_global_gc_interval_milliseconds(),
      "GCSServer.deadline_timer.gcs_global_gc");

  is_started_ = true;
}

void GcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";

    io_context_provider_.StopAllDedicatedIOContexts();

    ray_syncer_.reset();
    pubsub_handler_.reset();

    // Shutdown the rpc server
    rpc_server_.Shutdown();

    kv_manager_.reset();

    is_stopped_ = true;

    RAY_LOG(INFO) << "GCS server stopped.";
  }
}

void GcsServer::InitGcsNodeManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_node_manager_ = std::make_unique<GcsNodeManager>(
      gcs_publisher_.get(),
      gcs_table_storage_.get(),
      io_context_provider_.GetIOContext<GcsNodeManager>(),
      &raylet_client_pool_,
      rpc_server_.GetClusterId(),
      *ray_event_recorder_,
      config_.session_name);
  // Initialize by gcs tables data.
  gcs_node_manager_->Initialize(gcs_init_data);
  rpc_server_.RegisterService(std::make_unique<rpc::NodeInfoGrpcService>(
      io_context_provider_.GetIOContext<GcsNodeManager>(),
      *gcs_node_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InitGcsHealthCheckManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);
  auto node_death_callback = [this](const NodeID &node_id) {
    this->io_context_provider_.GetDefaultIOContext().post(
        [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id, nullptr); },
        "GcsServer.NodeDeathCallback");
  };

  gcs_healthcheck_manager_ = GcsHealthCheckManager::Create(
      io_context_provider_.GetDefaultIOContext(), node_death_callback);
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      auto remote_address =
          rpc::RayletClientPool::GenerateRayletAddress(item.first,
                                                       item.second.node_manager_address(),
                                                       item.second.node_manager_port());
      auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);
      gcs_healthcheck_manager_->AddNode(item.first, raylet_client->GetChannel());
    }
  }
}

void GcsServer::InitGcsResourceManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(cluster_resource_scheduler_ && cluster_lease_manager_);
  gcs_resource_manager_ = std::make_unique<GcsResourceManager>(
      io_context_provider_.GetDefaultIOContext(),
      cluster_resource_scheduler_->GetClusterResourceManager(),
      *gcs_node_manager_,
      kGCSNodeID,
      cluster_lease_manager_.get());

  // Initialize by gcs tables data.
  gcs_resource_manager_->Initialize(gcs_init_data);
  rpc_server_.RegisterService(std::make_unique<rpc::NodeResourceInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_resource_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));

  periodical_runner_->RunFnPeriodically(
      [this] {
        for (const auto &alive_node : gcs_node_manager_->GetAllAliveNodes()) {
          auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
              alive_node.first,
              alive_node.second->node_manager_address(),
              alive_node.second->node_manager_port());
          auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);

          // GetResourceLoad will also get usage. Historically it didn't.
          raylet_client->GetResourceLoad([this](auto &status, auto &&load_and_usage) {
            if (status.ok()) {
              // TODO(vitsai): Remove duplicate reporting to GcsResourceManager
              // after verifying that non-autoscaler paths are taken care of.
              // Currently, GcsResourceManager aggregates reporting from different
              // sources at different intervals, leading to an obviously inconsistent
              // view.
              //
              // Once autoscaler is completely moved to the new mode of consistent
              // per-node reporting, remove this if it is not needed anymore.
              gcs_resource_manager_->UpdateResourceLoads(load_and_usage.resources());
              gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(
                  std::move(*load_and_usage.mutable_resources()));
            } else {
              RAY_LOG_EVERY_N(WARNING, 10)
                  << "Failed to get the resource load: " << status.ToString();
            }
          });
        }
      },
      RayConfig::instance().gcs_pull_resource_loads_period_milliseconds(),
      "RayletLoadPulled");
}

void GcsServer::InitClusterResourceScheduler() {
  cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
      io_context_provider_.GetDefaultIOContext(),
      scheduling::NodeID(kGCSNodeID.Binary()),
      NodeResources(),
      /*is_node_available_fn=*/
      [](auto) { return true; },
      /*is_local_node_with_raylet=*/false);
}

void GcsServer::InitClusterLeaseManager() {
  RAY_CHECK(cluster_resource_scheduler_);
  cluster_lease_manager_ = std::make_unique<ClusterLeaseManager>(
      kGCSNodeID,
      *cluster_resource_scheduler_,
      /*get_node_info=*/
      [this](const NodeID &node_id) {
        return gcs_node_manager_->GetAliveNodeAddress(node_id);
      },
      /*announce_infeasible_task=*/nullptr,
      /*local_lease_manager=*/local_lease_manager_);
}

void GcsServer::InitGcsJobManager(
    const GcsInitData &gcs_init_data,
    ray::observability::MetricInterface &running_job_gauge,
    ray::observability::MetricInterface &finished_job_counter,
    ray::observability::MetricInterface &job_duration_in_seconds_gauge) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_job_manager_ =
      std::make_unique<GcsJobManager>(*gcs_table_storage_,
                                      *gcs_publisher_,
                                      *runtime_env_manager_,
                                      *function_manager_,
                                      kv_manager_->GetInstance(),
                                      io_context_provider_.GetDefaultIOContext(),
                                      worker_client_pool_,
                                      *ray_event_recorder_,
                                      config_.session_name,
                                      running_job_gauge,
                                      finished_job_counter,
                                      job_duration_in_seconds_gauge);
  gcs_job_manager_->Initialize(gcs_init_data);

  rpc_server_.RegisterService(std::make_unique<rpc::JobInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_job_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InitGcsActorManager(
    const GcsInitData &gcs_init_data,
    ray::observability::MetricInterface &actor_by_state_gauge,
    ray::observability::MetricInterface &gcs_actor_by_state_gauge) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_ && gcs_node_manager_);
  std::unique_ptr<GcsActorSchedulerInterface> scheduler;
  auto schedule_failure_handler =
      [this](std::shared_ptr<GcsActor> actor,
             const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
             const std::string &scheduling_failure_message) {
        // When there are no available nodes to schedule the actor the
        // gcs_actor_scheduler will treat it as failed and invoke this handler. In
        // this case, the actor manager should schedule the actor once an
        // eligible node is registered.
        gcs_actor_manager_->OnActorSchedulingFailed(
            std::move(actor), failure_type, scheduling_failure_message);
      };
  auto schedule_success_handler = [this](const std::shared_ptr<GcsActor> &actor,
                                         const rpc::PushTaskReply &reply) {
    gcs_actor_manager_->OnActorCreationSuccess(actor, reply);
  };

  RAY_CHECK(gcs_resource_manager_ && cluster_lease_manager_);
  scheduler = std::make_unique<GcsActorScheduler>(
      io_context_provider_.GetDefaultIOContext(),
      gcs_table_storage_->ActorTable(),
      *gcs_node_manager_,
      *cluster_lease_manager_,
      schedule_failure_handler,
      schedule_success_handler,
      raylet_client_pool_,
      worker_client_pool_,
      metrics_.scheduler_placement_time_s_histogram,
      /*normal_task_resources_changed_callback=*/
      [this](const NodeID &node_id, const rpc::ResourcesData &resources) {
        gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, resources);
      });
  gcs_actor_manager_ = std::make_unique<GcsActorManager>(
      std::move(scheduler),
      gcs_table_storage_.get(),
      io_context_provider_.GetDefaultIOContext(),
      gcs_publisher_.get(),
      *runtime_env_manager_,
      *function_manager_,
      [this](const ActorID &actor_id) {
        gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
      },
      raylet_client_pool_,
      worker_client_pool_,
      *ray_event_recorder_,
      config_.session_name,
      actor_by_state_gauge,
      gcs_actor_by_state_gauge);

  gcs_actor_manager_->Initialize(gcs_init_data);
  rpc_server_.RegisterService(std::make_unique<rpc::ActorInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_actor_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InitGcsPlacementGroupManager(
    const GcsInitData &gcs_init_data,
    ray::observability::MetricInterface &placement_group_gauge,
    ray::observability::MetricInterface &placement_group_creation_latency_in_ms_histogram,
    ray::observability::MetricInterface
        &placement_group_scheduling_latency_in_ms_histogram,
    ray::observability::MetricInterface &placement_group_count_gauge) {
  RAY_CHECK(gcs_table_storage_ && gcs_node_manager_);
  gcs_placement_group_scheduler_ = std::make_unique<GcsPlacementGroupScheduler>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_table_storage_,
      *gcs_node_manager_,
      *cluster_resource_scheduler_,
      raylet_client_pool_);

  gcs_placement_group_manager_ = std::make_unique<GcsPlacementGroupManager>(
      io_context_provider_.GetDefaultIOContext(),
      gcs_placement_group_scheduler_.get(),
      gcs_table_storage_.get(),
      *gcs_resource_manager_,
      [this](const JobID &job_id) {
        return gcs_job_manager_->GetJobConfig(job_id)->ray_namespace();
      },
      placement_group_gauge,
      placement_group_creation_latency_in_ms_histogram,
      placement_group_scheduling_latency_in_ms_histogram,
      placement_group_count_gauge);

  gcs_placement_group_manager_->Initialize(gcs_init_data);
  rpc_server_.RegisterService(std::make_unique<rpc::PlacementGroupInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_placement_group_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

GcsServer::StorageType GcsServer::GetStorageType() const {
  if (RayConfig::instance().gcs_storage() == kInMemoryStorage) {
    if (!config_.redis_address.empty()) {
      RAY_LOG(INFO) << "Using external Redis for KV storage: "
                    << BuildAddress(config_.redis_address, config_.redis_port);
      return StorageType::REDIS_PERSIST;
    }
    return StorageType::IN_MEMORY;
  }
  if (RayConfig::instance().gcs_storage() == kRedisStorage) {
    RAY_CHECK(!config_.redis_address.empty());
    return StorageType::REDIS_PERSIST;
  }
  RAY_LOG(FATAL) << "Unsupported GCS storage type: "
                 << RayConfig::instance().gcs_storage();
  return StorageType::UNKNOWN;
}

void GcsServer::InitRaySyncer(const GcsInitData &gcs_init_data) {
  ray_syncer_ = std::make_unique<syncer::RaySyncer>(
      io_context_provider_.GetIOContext<syncer::RaySyncer>(),
      kGCSNodeID.Binary(),
      [this](const NodeID &node_id) {
        gcs_healthcheck_manager_->MarkNodeHealthy(node_id);
      });
  ray_syncer_->Register(
      syncer::MessageType::RESOURCE_VIEW, nullptr, gcs_resource_manager_.get());
  ray_syncer_->Register(
      syncer::MessageType::COMMANDS, nullptr, gcs_resource_manager_.get());
  rpc_server_.RegisterService(std::make_unique<syncer::RaySyncerService>(
      *ray_syncer_, ray::rpc::AuthenticationTokenLoader::instance().GetToken()));
}

void GcsServer::InitFunctionManager() {
  function_manager_ = std::make_unique<GCSFunctionManager>(
      kv_manager_->GetInstance(), io_context_provider_.GetDefaultIOContext());
}

void GcsServer::InitUsageStatsClient() {
  usage_stats_client_ = std::make_unique<UsageStatsClient>(
      kv_manager_->GetInstance(), io_context_provider_.GetDefaultIOContext());

  gcs_worker_manager_->SetUsageStatsClient(usage_stats_client_.get());
  gcs_actor_manager_->SetUsageStatsClient(usage_stats_client_.get());
  gcs_placement_group_manager_->SetUsageStatsClient(usage_stats_client_.get());
  gcs_task_manager_->SetUsageStatsClient(usage_stats_client_.get());
}

void GcsServer::InitKVManager() {
  auto &io_context = io_context_provider_.GetIOContext<GcsInternalKVManager>();
  std::unique_ptr<StoreClient> store_client;
  switch (storage_type_) {
  case (StorageType::REDIS_PERSIST):
    store_client =
        std::make_unique<RedisStoreClient>(io_context, GetRedisClientOptions());
    break;
  case (StorageType::IN_MEMORY):
    store_client = std::make_unique<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(),
        metrics_.storage_operation_latency_in_ms_histogram,
        metrics_.storage_operation_count_counter);
    break;
  default:
    RAY_LOG(FATAL) << "Unexpected storage type! " << storage_type_;
  }

  kv_manager_ = std::make_unique<GcsInternalKVManager>(
      std::make_unique<StoreClientInternalKV>(std::move(store_client)),
      config_.raylet_config_list,
      io_context);

  kv_manager_->GetInstance().Put(
      "",
      kGcsPidKey,
      std::to_string(getpid()),
      /*overwrite=*/true,
      {[](bool added) {
         if (!added) {
           RAY_LOG(WARNING)
               << "Failed to put the GCS pid in the kv store. GCS process metrics "
                  "will not be emitted.";
         }
       },
       io_context_provider_.GetDefaultIOContext()});
}

void GcsServer::InitKVService() {
  RAY_CHECK(kv_manager_);
  rpc_server_.RegisterService(
      std::make_unique<rpc::InternalKVGrpcService>(
          io_context_provider_.GetIOContext<GcsInternalKVManager>(),
          *kv_manager_,
          /*max_active_rpcs_per_handler_=*/-1),
      false /* token_auth */);
}

void GcsServer::InitPubSubHandler() {
  auto &io_context = io_context_provider_.GetIOContext<pubsub::GcsPublisher>();
  pubsub_handler_ = std::make_unique<InternalPubSubHandler>(io_context, *gcs_publisher_);

  // This service is used to handle long poll requests, so we don't limit active RPCs.
  rpc_server_.RegisterService(std::make_unique<rpc::InternalPubSubGrpcService>(
      io_context, *pubsub_handler_, /*max_active_rpcs_per_handler_=*/-1));
}

void GcsServer::InitRuntimeEnvManager() {
  runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
      /*deleter=*/[this](const std::string &plugin_uri,
                         std::function<void(bool)> callback) {
        // A valid runtime env URI is of the form "protocol://hash".
        static constexpr std::string_view protocol_sep = "://";
        const std::string_view plugin_uri_view = plugin_uri;
        auto protocol_end_pos = plugin_uri_view.find(protocol_sep);
        if (protocol_end_pos == std::string::npos) {
          RAY_LOG(ERROR) << "Plugin URI must be of form "
                         << "<protocol>://<hash>, got " << plugin_uri_view;
          callback(/*successful=*/false);
        } else {
          const std::string_view protocol = plugin_uri_view.substr(0, protocol_end_pos);
          if (protocol != "gcs") {
            // Some URIs do not correspond to files in the GCS.  Skip deletion for
            // these.
            callback(/*successful=*/true);
          } else {
            this->kv_manager_->GetInstance().Del(
                "" /* namespace */,
                plugin_uri /* key */,
                false /* del_by_prefix*/,
                {[callback = std::move(callback)](int64_t) {
                   callback(/*successful=*/false);
                 },
                 io_context_provider_.GetDefaultIOContext()});
          }
        }
      });
  runtime_env_handler_ = std::make_unique<RuntimeEnvHandler>(
      io_context_provider_.GetDefaultIOContext(),
      *runtime_env_manager_, /*delay_executor=*/
      [this](std::function<void()> task, uint32_t delay_ms) {
        return execute_after(io_context_provider_.GetDefaultIOContext(),
                             std::move(task),
                             std::chrono::milliseconds(delay_ms));
      });
  rpc_server_.RegisterService(std::make_unique<rpc::RuntimeEnvGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *runtime_env_handler_,
      /*max_active_rpcs_per_handler=*/-1));
}

void GcsServer::InitGcsWorkerManager() {
  gcs_worker_manager_ = std::make_unique<GcsWorkerManager>(
      *gcs_table_storage_, io_context_provider_.GetDefaultIOContext(), *gcs_publisher_);
  rpc_server_.RegisterService(std::make_unique<rpc::WorkerInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_worker_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InitGcsAutoscalerStateManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(kv_manager_) << "kv_manager_ is not initialized.";
  auto v2_enabled =
      std::to_string(static_cast<int>(RayConfig::instance().enable_autoscaler_v2()));
  RAY_LOG(INFO) << "Autoscaler V2 enabled: " << v2_enabled;

  kv_manager_->GetInstance().Put(
      kGcsAutoscalerStateNamespace,
      kGcsAutoscalerV2EnabledKey,
      v2_enabled,
      /*overwrite=*/true,
      {[this, v2_enabled](bool new_value_put) {
         if (!new_value_put) {
           // NOTE(rickyx): We cannot know if an overwrite Put succeeds or fails (e.g.
           // when GCS re-started), so we just try to get the value to check if it's
           // correct.
           // TODO(rickyx): We could probably load some system configs from internal kv
           // when we initialize GCS from restart to avoid this.
           kv_manager_->GetInstance().Get(
               kGcsAutoscalerStateNamespace,
               kGcsAutoscalerV2EnabledKey,
               {[v2_enabled](std::optional<std::string> value) {
                  RAY_CHECK(value.has_value())
                      << "Autoscaler v2 feature flag wasn't found "
                         "in GCS, this is unexpected.";
                  RAY_CHECK(*value == v2_enabled) << "Autoscaler v2 feature flag in GCS "
                                                     "doesn't match the one we put.";
                },
                this->io_context_provider_.GetDefaultIOContext()});
         }
       },
       io_context_provider_.GetDefaultIOContext()});

  gcs_autoscaler_state_manager_ = std::make_unique<GcsAutoscalerStateManager>(
      config_.session_name,
      *gcs_node_manager_,
      *gcs_actor_manager_,
      *gcs_placement_group_manager_,
      raylet_client_pool_,
      kv_manager_->GetInstance(),
      io_context_provider_.GetDefaultIOContext(),
      gcs_publisher_.get());
  gcs_autoscaler_state_manager_->Initialize(gcs_init_data);
  rpc_server_.RegisterService(
      std::make_unique<rpc::autoscaler::AutoscalerStateGrpcService>(
          io_context_provider_.GetDefaultIOContext(),
          *gcs_autoscaler_state_manager_,
          RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InitGcsTaskManager(
    ray::observability::MetricInterface &task_events_reported_gauge,
    ray::observability::MetricInterface &task_events_dropped_gauge,
    ray::observability::MetricInterface &task_events_stored_gauge) {
  auto &io_context = io_context_provider_.GetIOContext<GcsTaskManager>();
  gcs_task_manager_ = std::make_unique<GcsTaskManager>(io_context,
                                                       task_events_reported_gauge,
                                                       task_events_dropped_gauge,
                                                       task_events_stored_gauge);
  // Register service.
  rpc_server_.RegisterService(std::make_unique<rpc::TaskInfoGrpcService>(
      io_context,
      *gcs_task_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
  rpc_server_.RegisterService(std::make_unique<rpc::events::RayEventExportGrpcService>(
      io_context,
      *gcs_task_manager_,
      RayConfig::instance().gcs_max_active_rpcs_per_handler()));
}

void GcsServer::InstallEventListeners() {
  // Install node event listeners.
  gcs_node_manager_->AddNodeAddedListener(
      [this](const std::shared_ptr<const rpc::GcsNodeInfo> &node) {
        // Because a new node has been added, we need to try to schedule the pending
        // placement groups and the pending actors.
        auto node_id = NodeID::FromBinary(node->node_id());
        gcs_resource_manager_->OnNodeAdd(*node);
        gcs_placement_group_manager_->OnNodeAdd(node_id);
        gcs_actor_manager_->SchedulePendingActors();
        gcs_autoscaler_state_manager_->OnNodeAdd(*node);
        auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
            node_id, node->node_manager_address(), node->node_manager_port());

        auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);

        if (gcs_healthcheck_manager_) {
          RAY_CHECK(raylet_client != nullptr);
          auto channel = raylet_client->GetChannel();
          RAY_CHECK(channel != nullptr);
          gcs_healthcheck_manager_->AddNode(node_id, channel);
        }
        cluster_lease_manager_->ScheduleAndGrantLeases();
      },
      io_context_provider_.GetDefaultIOContext());
  gcs_node_manager_->AddNodeRemovedListener(
      [this](const std::shared_ptr<const rpc::GcsNodeInfo> &node) {
        auto node_id = NodeID::FromBinary(node->node_id());
        const auto node_ip_address = node->node_manager_address();
        // All of the related placement groups and actors should be reconstructed when a
        // node is removed from the GCS.
        gcs_resource_manager_->OnNodeDead(node_id);
        gcs_placement_group_manager_->OnNodeDead(node_id);
        gcs_actor_manager_->OnNodeDead(node, node_ip_address);
        gcs_job_manager_->OnNodeDead(node_id);
        raylet_client_pool_.Disconnect(node_id);
        worker_client_pool_.Disconnect(node_id);
        gcs_healthcheck_manager_->RemoveNode(node_id);
        pubsub_handler_->AsyncRemoveSubscriberFrom(node_id.Binary());
        gcs_autoscaler_state_manager_->OnNodeDead(node_id);
      },
      io_context_provider_.GetDefaultIOContext());

  // Install worker event listener.
  gcs_worker_manager_->AddWorkerDeadListener(
      [this](const std::shared_ptr<rpc::WorkerTableData> &worker_failure_data) {
        auto &worker_address = worker_failure_data->worker_address();
        auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
        worker_client_pool_.Disconnect(worker_id);
        auto node_id = NodeID::FromBinary(worker_address.node_id());
        auto worker_ip = worker_address.ip_address();
        const rpc::RayException *creation_task_exception = nullptr;
        if (worker_failure_data->has_creation_task_exception()) {
          creation_task_exception = &worker_failure_data->creation_task_exception();
        }
        gcs_actor_manager_->OnWorkerDead(node_id,
                                         worker_id,
                                         worker_ip,
                                         worker_failure_data->exit_type(),
                                         worker_failure_data->exit_detail(),
                                         creation_task_exception);
        gcs_placement_group_scheduler_->HandleWaitingRemovedBundles();
        pubsub_handler_->AsyncRemoveSubscriberFrom(worker_id.Binary());
        gcs_task_manager_->OnWorkerDead(worker_id, worker_failure_data);
      });

  // Install job event listeners.
  gcs_job_manager_->AddJobFinishedListener([this](const rpc::JobTableData &job_data) {
    const auto job_id = JobID::FromBinary(job_data.job_id());
    gcs_task_manager_->OnJobFinished(job_id, job_data.end_time());
    gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  });

  // Install scheduling event listeners.
  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    gcs_resource_manager_->AddResourcesChangedListener([this] {
      io_context_provider_.GetDefaultIOContext().post(
          [this] {
            // Because resources have been changed, we need to try to schedule the
            // pending placement groups and actors.
            gcs_placement_group_manager_->SchedulePendingPlacementGroups();
            cluster_lease_manager_->ScheduleAndGrantLeases();
          },
          "GcsServer.SchedulePendingActors");
    });

    gcs_placement_group_scheduler_->AddResourcesChangedListener([this] {
      io_context_provider_.GetDefaultIOContext().post(
          [this] {
            // Because some placement group resources have been committed or deleted, we
            // need to try to schedule the pending placement groups and actors.
            gcs_placement_group_manager_->SchedulePendingPlacementGroups();
            cluster_lease_manager_->ScheduleAndGrantLeases();
          },
          "GcsServer.SchedulePendingPGActors");
    });
  }
}

void GcsServer::RecordMetrics() const {
  gcs_actor_manager_->RecordMetrics();
  gcs_placement_group_manager_->RecordMetrics();
  gcs_task_manager_->RecordMetrics();
  gcs_job_manager_->RecordMetrics();
}

void GcsServer::PrintDebugState() const {
  RAY_LOG(INFO) << "Gcs Debug state:\n\n"
                << gcs_node_manager_->DebugString() << "\n\n"
                << gcs_actor_manager_->DebugString() << "\n\n"
                << gcs_resource_manager_->DebugString() << "\n\n"
                << gcs_placement_group_manager_->DebugString() << "\n\n"
                << gcs_publisher_->DebugString() << "\n\n"
                << runtime_env_manager_->DebugString() << "\n\n"
                << gcs_task_manager_->DebugString() << "\n\n"
                << gcs_autoscaler_state_manager_->DebugString() << "\n\n";

  /// If periodic asio stats print is enabled, it will print it.
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    RAY_LOG(INFO) << "Main service Event stats:\n\n"
                  << io_context_provider_.GetDefaultIOContext().stats().StatsString()
                  << "\n\n";
    for (const auto &io_context : io_context_provider_.GetAllDedicatedIOContexts()) {
      RAY_LOG(INFO) << io_context->GetName() << " Event stats:\n\n"
                    << io_context->GetIoService().stats().StatsString() << "\n\n";
    }
  }
}

RedisClientOptions GcsServer::GetRedisClientOptions() {
  return RedisClientOptions{config_.redis_address,
                            config_.redis_port,
                            config_.redis_username,
                            config_.redis_password,
                            config_.enable_redis_ssl};
}

void GcsServer::TryGlobalGC() {
  if (cluster_lease_manager_->GetPendingQueueSize() == 0) {
    task_pending_schedule_detected_ = 0;
    return;
  }
  // Trigger global gc to solve task pending.
  // To avoid spurious triggers, only those after two consecutive
  // detections and under throttling are sent out (similar to
  // `NodeManager::WarnResourceDeadlock()`).
  if (task_pending_schedule_detected_++ > 0 && global_gc_throttler_->AbleToRun()) {
    syncer::CommandsSyncMessage commands_sync_message;
    commands_sync_message.set_should_global_gc(true);

    auto msg = std::make_shared<syncer::RaySyncMessage>();
    msg->set_version(absl::GetCurrentTimeNanos());
    msg->set_node_id(kGCSNodeID.Binary());
    msg->set_message_type(syncer::MessageType::COMMANDS);
    std::string serialized_msg;
    RAY_CHECK(commands_sync_message.SerializeToString(&serialized_msg));
    msg->set_sync_message(std::move(serialized_msg));
    ray_syncer_->BroadcastMessage(std::move(msg));
    global_gc_throttler_->RunNow();
  }
}

}  // namespace gcs
}  // namespace ray
