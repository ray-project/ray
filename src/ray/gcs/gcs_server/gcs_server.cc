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

#include "src/ray/gcs/gcs_server/gcs_server.h"

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "src/ray/common/asio/asio_util.h"
#include "src/ray/common/asio/instrumented_io_context.h"
#include "src/ray/common/ray_config.h"
#include "src/ray/gcs/gcs_server/gcs_actor_manager.h"
#include "src/ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"
#include "src/ray/gcs/gcs_server/gcs_job_manager.h"
#include "src/ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "src/ray/gcs/gcs_server/gcs_resource_manager.h"
#include "src/ray/gcs/gcs_server/gcs_worker_manager.h"
#include "src/ray/gcs/gcs_server/store_client_kv.h"
#include "src/ray/pubsub/publisher.h"
#include "src/ray/util/util.h"
#include "src/ray/common/status.h"
#include "src/ray/rpc/grpc_server.h"
#include "src/ray/util/logging.h"
#include "src/ray/gcs/gcs_server/config_manager.h"
#include "src/ray/gcs/gcs_server/load_balancer.h"
#include "src/ray/gcs/gcs_server/metrics_manager.h"
#include "src/ray/gcs/gcs_server/service_discovery.h"
#include "src/ray/gcs/gcs_server/state_sync.h"
#include "ray/gcs/gcs_server/debug_tools.h"

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

GcsServer::GcsServer(const std::string &address, int port)
    : address_(address),
      port_(port),
      io_context_(instrumented_io_context(1)),
      grpc_server_(nullptr),
      service_discovery_(nullptr),
      load_balancer_(nullptr),
      state_sync_(nullptr),
      config_manager_(nullptr),
      metrics_manager_(nullptr),
      debug_tools_(nullptr) {
  RAY_LOG(INFO) << "Initializing GCS server at " << address_ << ":" << port_;
}

GcsServer::~GcsServer() {
  RAY_LOG(INFO) << "GCS server shutting down.";
  if (debug_tools_) {
    RAY_LOG(INFO) << "Final health report:\n" << debug_tools_->GetHealthReport();
  }
  RAY_CHECK_OK(Stop());
}

Status GcsServer::Start() {
  RAY_LOG(INFO) << "Starting GCS server at " << address_ << ":" << port_;

  // Initialize gRPC server
  grpc_server_ = std::make_unique<rpc::GrpcServer>(address_, port_);
  RAY_CHECK_OK(grpc_server_->Start());

  // Initialize service discovery
  service_discovery_ = std::make_unique<ServiceDiscovery>(this);
  RAY_CHECK_OK(service_discovery_->Start());

  // Initialize load balancer
  load_balancer_ = std::make_unique<LoadBalancer>(this);
  RAY_CHECK_OK(load_balancer_->Start());

  // Initialize state sync
  state_sync_ = std::make_unique<StateSync>(this);
  RAY_CHECK_OK(state_sync_->Start());

  // Initialize config manager
  config_manager_ = std::make_unique<ConfigManager>(this);
  RAY_CHECK_OK(config_manager_->Start());

  // Initialize metrics manager
  metrics_manager_ = std::make_unique<MetricsManager>(this);
  RAY_CHECK_OK(metrics_manager_->Start());

  // Initialize debug tools
  debug_tools_ = std::make_unique<GcsDebugTools>(this);

  // Register service with metadata
  std::unordered_map<std::string, std::string> service_info = {
      {"service_type", "gcs"},
      {"version", "1.0.0"},
      {"address", address_},
      {"port", std::to_string(port_)}
  };
  RAY_CHECK_OK(service_discovery_->RegisterService("gcs", address_, port_, service_info));

  RAY_LOG(INFO) << "GCS server started successfully.";
  return Status::OK();
}

Status GcsServer::Stop() {
  RAY_LOG(INFO) << "Stopping GCS server...";

  // Stop components in reverse order of initialization
  if (debug_tools_) {
    RAY_LOG(INFO) << "Final health check: " << debug_tools_->CheckHealth().ToString();
    debug_tools_.reset();
  }

  if (metrics_manager_) {
    RAY_CHECK_OK(metrics_manager_->Stop());
    metrics_manager_.reset();
  }

  if (config_manager_) {
    RAY_CHECK_OK(config_manager_->Stop());
    config_manager_.reset();
  }

  if (state_sync_) {
    RAY_CHECK_OK(state_sync_->Stop());
    state_sync_.reset();
  }

  if (load_balancer_) {
    RAY_CHECK_OK(load_balancer_->Stop());
    load_balancer_.reset();
  }

  if (service_discovery_) {
    RAY_CHECK_OK(service_discovery_->Stop());
    service_discovery_.reset();
  }

  if (grpc_server_) {
    RAY_CHECK_OK(grpc_server_->Stop());
    grpc_server_.reset();
  }

  RAY_LOG(INFO) << "GCS server stopped.";
  return Status::OK();
}

RedisClientOptions GcsServer::GetRedisClientOptions() const {
  return RedisClientOptions(config_.redis_address,
                            config_.redis_port,
                            config_.redis_username,
                            config_.redis_password,
                            config_.enable_redis_ssl);
}

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

  // Start services
  service_discovery_->Start();
  load_balancer_->Start();
  state_sync_->Start();
  config_manager_->Start();
  monitoring_->Start();
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
           instrumented_io_context &io_context = continuation.io_context();
           ClusterID cluster_id = ClusterID::FromRandom();
           RAY_LOG(INFO) << "No existing server cluster ID found. Generating new ID: "
                         << cluster_id.Hex();
           kv_manager_->GetInstance().Put(
               kClusterIdNamespace,
               kClusterIdKey,
               cluster_id.Binary(),
               false,
               {[cluster_id,
                 continuation = std::move(continuation)](bool added_entry) mutable {
                  RAY_CHECK(added_entry) << "Failed to persist new cluster ID!";
                  std::move(continuation)
                      .Dispatch("GcsServer.GetOrGenerateClusterId.continuation",
                                cluster_id);
                },
                io_context});
         } else {
           ClusterID cluster_id = ClusterID::FromBinary(provided_cluster_id.value());
           RAY_LOG(INFO) << "Found existing server token: " << cluster_id;
           std::move(continuation)
               .Dispatch("GcsServer.GetOrGenerateClusterId.continuation", cluster_id);
         }
       },
       io_context});
}

void GcsServer::DoStart(const GcsInitData &gcs_init_data) {
  // Init cluster resource scheduler.
  InitClusterResourceScheduler();

  // Init gcs node manager.
  InitGcsNodeManager(gcs_init_data);

  // Init cluster task manager.
  InitClusterTaskManager();

  // Init gcs resource manager.
  InitGcsResourceManager(gcs_init_data);

  // Init gcs health check manager.
  InitGcsHealthCheckManager(gcs_init_data);

  // Init synchronization service
  InitRaySyncer(gcs_init_data);

  // Init KV service.
  InitKVService();

  // Init function manager
  InitFunctionManager();

  // Init Pub/Sub handler
  InitPubSubHandler();

  // Init RuntimeEnv manager
  InitRuntimeEnvManager();

  // Init gcs job manager.
  InitGcsJobManager(gcs_init_data);

  // Init gcs placement group manager.
  InitGcsPlacementGroupManager(gcs_init_data);

  // Init gcs actor manager.
  InitGcsActorManager(gcs_init_data);

  // Init gcs worker manager.
  InitGcsWorkerManager();

  // Init GCS task manager.
  InitGcsTaskManager();

  // Install event listeners.
  InstallEventListeners();

  // Init autoscaling manager
  InitGcsAutoscalerStateManager(gcs_init_data);

  // Init usage stats client.
  InitUsageStatsClient();

  // Initialize Raft service
  raft_service_handler_ = std::make_unique<RaftServiceHandler>(*this);
  raft_service_ = std::make_unique<rpc::RaftGrpcService>(io_context_provider_.GetIOContext(),
                                                        *raft_service_handler_);
  rpc_server_.RegisterService(*raft_service_);

  RecordMetrics();

  // Start RPC server when all tables have finished loading initial
  // data.
  rpc_server_.Run();

  periodical_runner_->RunFnPeriodically(
      [this] {
        RAY_LOG(INFO) << GetDebugState();
        PrintAsioStats();
      },
      /*ms*/ RayConfig::instance().event_stats_print_interval_ms(),
      "GCSServer.deadline_timer.debug_state_event_stats_print");

  global_gc_throttler_ =
      std::make_unique<Throttler>(RayConfig::instance().global_gc_min_interval_s() * 1e9);

  periodical_runner_->RunFnPeriodically(
      [this] {
        DumpDebugStateToFile();
        TryGlobalGC();
      },
      /*ms*/ RayConfig::instance().debug_dump_period_milliseconds(),
      "GCSServer.deadline_timer.debug_state_dump");

  is_started_ = true;
}

void GcsServer::InitGcsNodeManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_node_manager_ =
      std::make_unique<GcsNodeManager>(gcs_publisher_.get(),
                                       gcs_table_storage_.get(),
                                       io_context_provider_.GetDefaultIOContext(),
                                       raylet_client_pool_.get(),
                                       rpc_server_.GetClusterId());
  // Initialize by gcs tables data.
  gcs_node_manager_->Initialize(gcs_init_data);
  // Register service.
  node_info_service_ = std::make_unique<rpc::NodeInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(), *gcs_node_manager_);
  rpc_server_.RegisterService(*node_info_service_);
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
      rpc::Address remote_address;
      remote_address.set_raylet_id(item.second.node_id());
      remote_address.set_ip_address(item.second.node_manager_address());
      remote_address.set_port(item.second.node_manager_port());
      auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
      gcs_healthcheck_manager_->AddNode(item.first, raylet_client->GetChannel());
    }
  }
}

void GcsServer::InitGcsResourceManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(cluster_resource_scheduler_ && cluster_task_manager_);
  gcs_resource_manager_ = std::make_unique<GcsResourceManager>(
      io_context_provider_.GetDefaultIOContext(),
      cluster_resource_scheduler_->GetClusterResourceManager(),
      *gcs_node_manager_,
      kGCSNodeID,
      cluster_task_manager_.get());

  // Initialize by gcs tables data.
  gcs_resource_manager_->Initialize(gcs_init_data);
  // Register service.
  node_resource_info_service_.reset(new rpc::NodeResourceInfoGrpcService(
      io_context_provider_.GetDefaultIOContext(), *gcs_resource_manager_));
  rpc_server_.RegisterService(*node_resource_info_service_);

  periodical_runner_->RunFnPeriodically(
      [this] {
        for (const auto &alive_node : gcs_node_manager_->GetAllAliveNodes()) {
          std::shared_ptr<ray::RayletClientInterface> raylet_client;
          // GetOrConnectionByID will not connect to the raylet is it hasn't been
          // connected.
          if (auto conn_opt = raylet_client_pool_->GetOrConnectByID(alive_node.first)) {
            raylet_client = *conn_opt;
          } else {
            // When not connect, use GetOrConnectByAddress
            rpc::Address remote_address;
            remote_address.set_raylet_id(alive_node.second->node_id());
            remote_address.set_ip_address(alive_node.second->node_manager_address());
            remote_address.set_port(alive_node.second->node_manager_port());
            raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
          }
          if (raylet_client == nullptr) {
            RAY_LOG(ERROR) << "Failed to connect to node: " << alive_node.first
                           << ". Skip this round of pulling for resource load";
          } else {
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
                    std::move(load_and_usage.resources()));
              } else {
                RAY_LOG_EVERY_N(WARNING, 10)
                    << "Failed to get the resource load: " << status.ToString();
              }
            });
          }
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

void GcsServer::InitClusterTaskManager() {
  RAY_CHECK(cluster_resource_scheduler_);
  cluster_task_manager_ = std::make_unique<ClusterTaskManager>(
      kGCSNodeID,
      *cluster_resource_scheduler_,
      /*get_node_info=*/
      [this](const NodeID &node_id) {
        auto node = gcs_node_manager_->GetAliveNode(node_id);
        return node.has_value() ? node.value().get() : nullptr;
      },
      /*announce_infeasible_task=*/nullptr,
      /*local_task_manager=*/local_task_manager_);
}

void GcsServer::InitGcsJobManager(const GcsInitData &gcs_init_data) {
  auto client_factory = [this](const rpc::Address &address) {
    return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_, []() {
      RAY_LOG(FATAL) << "GCS doesn't call any retryable core worker grpc methods.";
    });
  };
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_job_manager_ =
      std::make_unique<GcsJobManager>(*gcs_table_storage_,
                                      *gcs_publisher_,
                                      *runtime_env_manager_,
                                      *function_manager_,
                                      kv_manager_->GetInstance(),
                                      io_context_provider_.GetDefaultIOContext(),
                                      client_factory);
  gcs_job_manager_->Initialize(gcs_init_data);

  // Register service.
  job_info_service_ = std::make_unique<rpc::JobInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(), *gcs_job_manager_);
  rpc_server_.RegisterService(*job_info_service_);
}

void GcsServer::InitGcsActorManager(const GcsInitData &gcs_init_data) {
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

  RAY_CHECK(gcs_resource_manager_ && cluster_task_manager_);
  scheduler =
      std::make_unique<GcsActorScheduler>(
          io_context_provider_.GetDefaultIOContext(),
          gcs_table_storage_->ActorTable(),
          *gcs_node_manager_,
          *cluster_task_manager_,
          schedule_failure_handler,
          schedule_success_handler,
          *raylet_client_pool_,
          /*factory=*/
          [this](const rpc::Address &address) {
            return std::make_shared<rpc::CoreWorkerClient>(
                address, client_call_manager_, []() {
                  RAY_LOG(FATAL)
                      << "GCS doesn't call any retryable core worker grpc methods.";
                });
          },
          /*normal_task_resources_changed_callback=*/
          [this](const NodeID &node_id, const rpc::ResourcesData &resources) {
            gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, resources);
          });
  gcs_actor_manager_ =
      std::make_unique<GcsActorManager>(
          std::move(scheduler),
          gcs_table_storage_.get(),
          io_context_provider_.GetDefaultIOContext(),
          gcs_publisher_.get(),
          *runtime_env_manager_,
          *function_manager_,
          [this](const ActorID &actor_id) {
            gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(
                actor_id);
          },
          [this](const rpc::Address &address) {
            return std::make_shared<rpc::CoreWorkerClient>(
                address, client_call_manager_, []() {
                  RAY_LOG(FATAL)
                      << "GCS doesn't call any retryable core worker grpc methods.";
                });
          });

  // Initialize by gcs tables data.
  gcs_actor_manager_->Initialize(gcs_init_data);
  // Register service.
  actor_info_service_ = std::make_unique<rpc::ActorInfoGrpcService>(
      io_context_provider_.GetDefaultIOContext(), *gcs_actor_manager_);
  rpc_server_.RegisterService(*actor_info_service_);
}

void GcsServer::InitGcsPlacementGroupManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_node_manager_);
  gcs_placement_group_scheduler_ = std::make_unique<GcsPlacementGroupScheduler>(
      io_context_provider_.GetDefaultIOContext(),
      *gcs_table_storage_,
      *gcs_node_manager_,
      *cluster_resource_scheduler_,
      *raylet_client_pool_);

  gcs_placement_group_manager_ = std::make_unique<GcsPlacementGroupManager>(
      io_context_provider_.GetDefaultIOContext(),
      gcs_placement_group_scheduler_.get(),
      gcs_table_storage_.get(),
      *gcs_resource_manager_,
      [this](const JobID &job_id) {
        return gcs_job_manager_->GetJobConfig(job_id)->ray_namespace();
      });
  // Initialize by gcs tables data.
  gcs_placement_group_manager_->Initialize(gcs_init_data);
  // Register service.
  placement_group_info_service_.reset(new rpc::PlacementGroupInfoGrpcService(
      io_context_provider_.GetDefaultIOContext(), *gcs_placement_group_manager_));
  rpc_server_.RegisterService(*placement_group_info_service_);
}

GcsServer::StorageType GcsServer::GetStorageType() const {
  if (RayConfig::instance().gcs_storage() == kInMemoryStorage) {
    if (!config_.redis_address.empty()) {
      RAY_LOG(INFO) << "Using external Redis for KV storage: " << config_.redis_address
                    << ":" << config_.redis_port;
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
  ray_syncer_service_ = std::make_unique<syncer::RaySyncerService>(*ray_syncer_);
  rpc_server_.RegisterService(*ray_syncer_service_);
}

void GcsServer::InitFunctionManager() {
  function_manager_ = std::make_unique<GcsFunctionManager>(
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
  // TODO(yic): Use a factory with configs
  std::unique_ptr<InternalKVInterface> instance;
  auto &io_context = io_context_provider_.GetIOContext<GcsInternalKVManager>();
  switch (storage_type_) {
  case (StorageType::REDIS_PERSIST):
    instance = std::make_unique<StoreClientInternalKV>(
        std::make_unique<RedisStoreClient>(CreateRedisClient(io_context)));
    break;
  case (StorageType::IN_MEMORY):
    instance = std::make_unique<StoreClientInternalKV>(
        std::make_unique<ObservableStoreClient>(std::make_unique<InMemoryStoreClient>()));
    break;
  default:
    RAY_LOG(FATAL) << "Unexpected storage type! " << storage_type_;
  }

  kv_manager_ = std::make_unique<GcsInternalKVManager>(
      std::move(instance), config_.raylet_config_list, io_context);
}

void GcsServer::InitKVService() {
  RAY_CHECK(kv_manager_);
  kv_service_ = std::make_unique<rpc::InternalKVGrpcService>(
      io_context_provider_.GetIOContext<GcsInternalKVManager>(), *kv_manager_);
  // Register service.
  rpc_server_.RegisterService(*kv_service_, false /* token_auth */);
}

void GcsServer::InitPubSubHandler() {
  auto &io_context = io_context_provider_.GetIOContext<GcsPublisher>();
  pubsub_handler_ = std::make_unique<InternalPubSubHandler>(io_context, *gcs_publisher_);
  pubsub_service_ =
      std::make_unique<rpc::InternalPubSubGrpcService>(io_context, *pubsub_handler_);
  // Register service.
  rpc_server_.RegisterService(*pubsub_service_);
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
  runtime_env_service_ = std::make_unique<rpc::RuntimeEnvGrpcService>(
      io_context_provider_.GetDefaultIOContext(), *runtime_env_handler_);
  // Register service.
  rpc_server_.RegisterService(*runtime_env_service_);
}

void GcsServer::InitGcsWorkerManager() {
  gcs_worker_manager_ = std::make_unique<GcsWorkerManager>(
      *gcs_table_storage_, io_context_provider_.GetDefaultIOContext(), *gcs_publisher_);
  // Register service.
  worker_info_service_.reset(new rpc::WorkerInfoGrpcService(
      io_context_provider_.GetDefaultIOContext(), *gcs_worker_manager_));
  rpc_server_.RegisterService(*worker_info_service_);
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
           // NOTE(rickyx): We cannot know if an overwirte Put succeeds or fails (e.g.
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
      *raylet_client_pool_,
      kv_manager_->GetInstance(),
      io_context_provider_.GetDefaultIOContext(),
      gcs_publisher_.get());
  gcs_autoscaler_state_manager_->Initialize(gcs_init_data);

  autoscaler_state_service_.reset(new rpc::autoscaler::AutoscalerStateGrpcService(
      io_context_provider_.GetDefaultIOContext(), *gcs_autoscaler_state_manager_));

  rpc_server_.RegisterService(*autoscaler_state_service_);
}

void GcsServer::InitGcsTaskManager() {
  auto &io_context = io_context_provider_.GetIOContext<GcsTaskManager>();
  gcs_task_manager_ = std::make_unique<GcsTaskManager>(io_context);
  // Register service.
  task_info_service_.reset(new rpc::TaskInfoGrpcService(io_context, *gcs_task_manager_));
  rpc_server_.RegisterService(*task_info_service_);
}

void GcsServer::InstallEventListeners() {
  // Install node event listeners.
  gcs_node_manager_->AddNodeAddedListener(
      [this](const std::shared_ptr<rpc::GcsNodeInfo> &node) {
        // Because a new node has been added, we need to try to schedule the pending
        // placement groups and the pending actors.
        auto node_id = NodeID::FromBinary(node->node_id());
        gcs_resource_manager_->OnNodeAdd(*node);
        gcs_placement_group_manager_->OnNodeAdd(node_id);
        gcs_actor_manager_->SchedulePendingActors();
        gcs_autoscaler_state_manager_->OnNodeAdd(*node);
        rpc::Address address;
        address.set_raylet_id(node->node_id());
        address.set_ip_address(node->node_manager_address());
        address.set_port(node->node_manager_port());

        auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(address);

        if (gcs_healthcheck_manager_) {
          RAY_CHECK(raylet_client != nullptr);
          auto channel = raylet_client->GetChannel();
          RAY_CHECK(channel != nullptr);
          gcs_healthcheck_manager_->AddNode(node_id, channel);
        }
        cluster_task_manager_->ScheduleAndDispatchTasks();
      });
  gcs_node_manager_->AddNodeRemovedListener(
      [this](const std::shared_ptr<rpc::GcsNodeInfo> &node) {
        auto node_id = NodeID::FromBinary(node->node_id());
        const auto node_ip_address = node->node_manager_address();
        // All of the related placement groups and actors should be reconstructed when a
        // node is removed from the GCS.
        gcs_resource_manager_->OnNodeDead(node_id);
        gcs_placement_group_manager_->OnNodeDead(node_id);
        gcs_actor_manager_->OnNodeDead(node, node_ip_address);
        gcs_job_manager_->OnNodeDead(node_id);
        raylet_client_pool_->Disconnect(node_id);
        gcs_healthcheck_manager_->RemoveNode(node_id);
        pubsub_handler_->RemoveSubscriberFrom(node_id.Binary());
        gcs_autoscaler_state_manager_->OnNodeDead(node_id);
      });

  // Install worker event listener.
  gcs_worker_manager_->AddWorkerDeadListener(
      [this](const std::shared_ptr<rpc::WorkerTableData> &worker_failure_data) {
        auto &worker_address = worker_failure_data->worker_address();
        auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
        auto node_id = NodeID::FromBinary(worker_address.raylet_id());
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
        pubsub_handler_->RemoveSubscriberFrom(worker_id.Binary());
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
            cluster_task_manager_->ScheduleAndDispatchTasks();
          },
          "GcsServer.SchedulePendingActors");
    });

    gcs_placement_group_scheduler_->AddResourcesChangedListener([this] {
      io_context_provider_.GetDefaultIOContext().post(
          [this] {
            // Because some placement group resources have been committed or deleted, we
            // need to try to schedule the pending placement groups and actors.
            gcs_placement_group_manager_->SchedulePendingPlacementGroups();
            cluster_task_manager_->ScheduleAndDispatchTasks();
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
  execute_after(
      io_context_provider_.GetDefaultIOContext(),
      [this] { RecordMetrics(); },
      std::chrono::milliseconds(RayConfig::instance().metrics_report_interval_ms() /
                                2) /* milliseconds */);
}

void GcsServer::DumpDebugStateToFile() const {
  std::fstream fs;
  fs.open(config_.log_dir + "/debug_state_gcs.txt",
          std::fstream::out | std::fstream::trunc);
  fs << GetDebugState() << "\n\n";
  fs << io_context_provider_.GetDefaultIOContext().stats().StatsString();
  fs.close();
}

std::string GcsServer::GetDebugState() const {
  std::ostringstream stream;
  stream << "Gcs Debug state:\n\n"
         << gcs_node_manager_->DebugString() << "\n\n"
         << gcs_actor_manager_->DebugString() << "\n\n"
         << gcs_resource_manager_->DebugString() << "\n\n"
         << gcs_placement_group_manager_->DebugString() << "\n\n"
         << gcs_publisher_->DebugString() << "\n\n"
         << runtime_env_manager_->DebugString() << "\n\n"
         << gcs_task_manager_->DebugString() << "\n\n"
         << gcs_autoscaler_state_manager_->DebugString() << "\n\n";
  return stream.str();
}

std::shared_ptr<RedisClient> GcsServer::CreateRedisClient(
    instrumented_io_context &io_service) {
  auto redis_client = std::make_shared<RedisClient>(GetRedisClientOptions());
  auto status = redis_client->Connect(io_service);
  RAY_CHECK_OK(status) << "Failed to init redis gcs client";
  return redis_client;
}

void GcsServer::PrintAsioStats() {
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

void GcsServer::TryGlobalGC() {
  if (cluster_task_manager_->GetPendingQueueSize() == 0) {
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
