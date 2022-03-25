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

#include "ray/gcs/gcs_server/gcs_server.h"

#include <fstream>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"
#include "ray/gcs/gcs_server/gcs_worker_manager.h"
#include "ray/gcs/gcs_server/stats_handler_impl.h"
#include "ray/pubsub/publisher.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config,
                     instrumented_io_context &main_service)
    : config_(config),
      storage_type_(StorageType()),
      main_service_(main_service),
      rpc_server_(config.grpc_server_name,
                  config.grpc_server_port,
                  config.node_ip_address == "127.0.0.1",
                  config.grpc_server_thread_num,
                  /*keepalive_time_ms=*/RayConfig::instance().grpc_keepalive_time_ms()),
      client_call_manager_(main_service,
                           RayConfig::instance().gcs_server_rpc_client_thread_num()),
      raylet_client_pool_(
          std::make_shared<rpc::NodeManagerClientPool>(client_call_manager_)),
      pubsub_periodical_runner_(pubsub_io_service_),
      periodical_runner_(main_service),
      is_started_(false),
      is_stopped_(false) {
  // Init GCS table storage.
  RAY_LOG(INFO) << "GCS storage type is " << storage_type_;
  RAY_LOG(INFO) << "gRPC based pubsub is"
                << (RayConfig::instance().gcs_grpc_based_pubsub() ? " " : " not ")
                << "enabled";
  if (storage_type_ == "redis") {
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(GetOrConnectRedis());
  } else if (storage_type_ == "memory") {
    RAY_CHECK(RayConfig::instance().gcs_grpc_based_pubsub())
        << " grpc pubsub has to be enabled when using storage other than redis";
    gcs_table_storage_ = std::make_shared<InMemoryGcsTableStorage>(main_service_);
  }

  auto on_done = [this](const ray::Status &status) {
    RAY_CHECK(status.ok()) << "Failed to put internal config";
    this->main_service_.stop();
  };
  ray::rpc::StoredConfig stored_config;
  stored_config.set_config(config_.raylet_config_list);
  RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Put(
      ray::UniqueID::Nil(), stored_config, on_done));
  // Here we need to make sure the Put of internal config is happening in sync
  // way. But since the storage API is async, we need to run the main_service_
  // to block currenct thread.
  // This will run async operations from InternalConfigTable().Put() above
  // inline.
  main_service_.run();
  // Reset the main service to the initial status otherwise, the signal handler
  // will be called.
  main_service_.restart();

  // Init GCS publisher instance.
  std::unique_ptr<pubsub::Publisher> inner_publisher;
  if (config_.grpc_pubsub_enabled) {
    // Init grpc based pubsub on GCS.
    // TODO: Move this into GcsPublisher.
    inner_publisher = std::make_unique<pubsub::Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{
            rpc::ChannelType::GCS_ACTOR_CHANNEL,
            rpc::ChannelType::GCS_JOB_CHANNEL,
            rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
            rpc::ChannelType::GCS_NODE_RESOURCE_CHANNEL,
            rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
            rpc::ChannelType::RAY_ERROR_INFO_CHANNEL,
            rpc::ChannelType::RAY_LOG_CHANNEL,
            rpc::ChannelType::RAY_PYTHON_FUNCTION_CHANNEL,
            rpc::ChannelType::RAY_NODE_RESOURCE_USAGE_CHANNEL,
        },
        /*periodical_runner=*/&pubsub_periodical_runner_,
        /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
        /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
        /*publish_batch_size_=*/RayConfig::instance().publish_batch_size());
  }
  gcs_publisher_ =
      std::make_shared<GcsPublisher>(redis_client_, std::move(inner_publisher));
}

GcsServer::~GcsServer() { Stop(); }

RedisClientOptions GcsServer::GetRedisClientOptions() const {
  return RedisClientOptions(config_.redis_address,
                            config_.redis_port,
                            config_.redis_password,
                            config_.enable_sharding_conn);
}

void GcsServer::Start() {
  // Load gcs tables data asynchronously.
  auto gcs_init_data = std::make_shared<GcsInitData>(gcs_table_storage_);
  gcs_init_data->AsyncLoad([this, gcs_init_data] { DoStart(*gcs_init_data); });
}

void GcsServer::DoStart(const GcsInitData &gcs_init_data) {
  // Init cluster resource scheduler.
  InitClusterResourceScheduler();

  // Init gcs resource manager.
  InitGcsResourceManager(gcs_init_data);

  // Init synchronization service
  InitRaySyncer(gcs_init_data);

  // Init gcs node manager.
  InitGcsNodeManager(gcs_init_data);

  // Init gcs heartbeat manager.
  InitGcsHeartbeatManager(gcs_init_data);

  // Init KV Manager
  InitKVManager();

  // Init function manager
  InitFunctionManager();

  // Init Pub/Sub handler
  InitPubSubHandler();

  // Init RuntimeENv manager
  InitRuntimeEnvManager();

  // Init gcs job manager.
  InitGcsJobManager(gcs_init_data);

  // Init gcs placement group manager.
  InitGcsPlacementGroupManager(gcs_init_data);

  // Init gcs actor manager.
  InitGcsActorManager(gcs_init_data);

  // Init gcs worker manager.
  InitGcsWorkerManager();

  // Init stats handler.
  InitStatsHandler();

  // Install event listeners.
  InstallEventListeners();

  // Start RPC server when all tables have finished loading initial
  // data.
  rpc_server_.Run();

  if (!RayConfig::instance().bootstrap_with_gcs()) {
    StoreGcsServerAddressInRedis();
  }

  // Only after the rpc_server_ is running can the heartbeat manager
  // be run. Otherwise the node failure detector will mistake
  // some living nodes as dead as the timer inside node failure
  // detector is already run.
  gcs_heartbeat_manager_->Start();

  RecordMetrics();

  periodical_runner_.RunFnPeriodically(
      [this] {
        RAY_LOG(INFO) << GetDebugState();
        PrintAsioStats();
      },
      /*ms*/ RayConfig::instance().event_stats_print_interval_ms(),
      "GCSServer.deadline_timer.debug_state_event_stats_print");

  periodical_runner_.RunFnPeriodically(
      [this] { DumpDebugStateToFile(); },
      /*ms*/ RayConfig::instance().debug_dump_period_milliseconds(),
      "GCSServer.deadline_timer.debug_state_dump");

  is_started_ = true;
}

void GcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";
    // GcsHeartbeatManager should be stopped before RPCServer.
    // Because closing RPC server will cost several seconds, during this time,
    // GcsHeartbeatManager is still checking nodes' heartbeat timeout. Since RPC Server
    // won't handle heartbeat calls anymore, some nodes will be marked as dead during this
    // time, causing many nodes die after GCS's failure.
    gcs_heartbeat_manager_->Stop();

    ray_syncer_->Stop();

    // Shutdown the rpc server
    rpc_server_.Shutdown();

    pubsub_handler_->Stop();
    kv_manager_.reset();

    is_stopped_ = true;
    RAY_LOG(INFO) << "GCS server stopped.";
  }
}

void GcsServer::InitGcsNodeManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_node_manager_ = std::make_shared<GcsNodeManager>(
      gcs_publisher_, gcs_table_storage_, raylet_client_pool_);
  // Initialize by gcs tables data.
  gcs_node_manager_->Initialize(gcs_init_data);
  // Register service.
  node_info_service_.reset(
      new rpc::NodeInfoGrpcService(main_service_, *gcs_node_manager_));
  rpc_server_.RegisterService(*node_info_service_);
}

void GcsServer::InitGcsHeartbeatManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);
  gcs_heartbeat_manager_ = std::make_shared<GcsHeartbeatManager>(
      heartbeat_manager_io_service_, /*on_node_death_callback=*/
      [this](const NodeID &node_id) {
        main_service_.post(
            [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id); },
            "GcsServer.NodeDeathCallback");
      });
  // Initialize by gcs tables data.
  gcs_heartbeat_manager_->Initialize(gcs_init_data);
  // Register service.
  heartbeat_info_service_.reset(new rpc::HeartbeatInfoGrpcService(
      heartbeat_manager_io_service_, *gcs_heartbeat_manager_));
  rpc_server_.RegisterService(*heartbeat_info_service_);
}

void GcsServer::InitGcsResourceManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && cluster_resource_scheduler_);
  gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
      gcs_table_storage_, cluster_resource_scheduler_->GetClusterResourceManager());

  // Initialize by gcs tables data.
  gcs_resource_manager_->Initialize(gcs_init_data);
  // Register service.
  node_resource_info_service_.reset(
      new rpc::NodeResourceInfoGrpcService(main_service_, *gcs_resource_manager_));
  rpc_server_.RegisterService(*node_resource_info_service_);
}

void GcsServer::InitClusterResourceScheduler() {
  cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>();
}

void GcsServer::InitGcsJobManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_job_manager_ = std::make_unique<GcsJobManager>(
      gcs_table_storage_, gcs_publisher_, *runtime_env_manager_, *function_manager_);
  gcs_job_manager_->Initialize(gcs_init_data);

  // Register service.
  job_info_service_ =
      std::make_unique<rpc::JobInfoGrpcService>(main_service_, *gcs_job_manager_);
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
  auto schedule_success_handler = [this](std::shared_ptr<GcsActor> actor,
                                         const rpc::PushTaskReply &reply) {
    gcs_actor_manager_->OnActorCreationSuccess(std::move(actor), reply);
  };
  auto client_factory = [this](const rpc::Address &address) {
    return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
  };

  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    RAY_CHECK(gcs_resource_manager_ && cluster_resource_scheduler_);
    scheduler = std::make_unique<GcsBasedActorScheduler>(
        main_service_,
        gcs_table_storage_->ActorTable(),
        *gcs_node_manager_,
        cluster_resource_scheduler_,
        schedule_failure_handler,
        schedule_success_handler,
        raylet_client_pool_,
        client_factory,
        /*normal_task_resources_changed_callback=*/
        [this](const NodeID &node_id, const rpc::ResourcesData &resources) {
          gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, resources);
        });
  } else {
    scheduler =
        std::make_unique<RayletBasedActorScheduler>(main_service_,
                                                    gcs_table_storage_->ActorTable(),
                                                    *gcs_node_manager_,
                                                    schedule_failure_handler,
                                                    schedule_success_handler,
                                                    raylet_client_pool_,
                                                    client_factory);
  }
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(
      main_service_,
      std::move(scheduler),
      gcs_table_storage_,
      gcs_publisher_,
      *runtime_env_manager_,
      *function_manager_,
      [this](const ActorID &actor_id) {
        gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
      },
      /*get_job_config_func=*/
      [this](const JobID &job_id) { return gcs_job_manager_->GetJobConfig(job_id); },
      [this](std::function<void(void)> fn, boost::posix_time::milliseconds delay) {
        boost::asio::deadline_timer timer(main_service_);
        timer.expires_from_now(delay);
        timer.async_wait([fn](const boost::system::error_code &error) {
          if (error != boost::asio::error::operation_aborted) {
            fn();
          } else {
            RAY_LOG(WARNING)
                << "The GCS actor metadata garbage collector timer failed to fire. This "
                   "could old actor metadata not being properly cleaned up. For more "
                   "information, check logs/gcs_server.err and logs/gcs_server.out";
          }
        });
      },
      [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      });

  // Initialize by gcs tables data.
  gcs_actor_manager_->Initialize(gcs_init_data);
  // Register service.
  actor_info_service_.reset(
      new rpc::ActorInfoGrpcService(main_service_, *gcs_actor_manager_));
  rpc_server_.RegisterService(*actor_info_service_);
}

void GcsServer::InitGcsPlacementGroupManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_node_manager_);
  auto scheduler =
      std::make_shared<GcsPlacementGroupScheduler>(main_service_,
                                                   gcs_table_storage_,
                                                   *gcs_node_manager_,
                                                   *gcs_resource_manager_,
                                                   *cluster_resource_scheduler_,
                                                   raylet_client_pool_,
                                                   *ray_syncer_);

  gcs_placement_group_manager_ = std::make_shared<GcsPlacementGroupManager>(
      main_service_,
      scheduler,
      gcs_table_storage_,
      *gcs_resource_manager_,
      [this](const JobID &job_id) {
        return gcs_job_manager_->GetJobConfig(job_id)->ray_namespace();
      });
  // Initialize by gcs tables data.
  gcs_placement_group_manager_->Initialize(gcs_init_data);
  // Register service.
  placement_group_info_service_.reset(new rpc::PlacementGroupInfoGrpcService(
      main_service_, *gcs_placement_group_manager_));
  rpc_server_.RegisterService(*placement_group_info_service_);
}

std::string GcsServer::StorageType() const {
  if (RayConfig::instance().gcs_storage() == "memory") {
    if (!config_.redis_address.empty()) {
      RAY_LOG(INFO) << "Using external Redis for KV storage: " << config_.redis_address;
      return "redis";
    }
    return "memory";
  }
  if (RayConfig::instance().gcs_storage() == "redis") {
    RAY_CHECK(!config_.redis_address.empty());
    return "redis";
  }
  RAY_LOG(FATAL) << "Unsupported GCS storage type: "
                 << RayConfig::instance().gcs_storage();
  return RayConfig::instance().gcs_storage();
}

void GcsServer::StoreGcsServerAddressInRedis() {
  std::string ip = config_.node_ip_address;
  if (ip.empty()) {
    ip = GetValidLocalIp(
        GetPort(),
        RayConfig::instance().internal_gcs_service_connect_wait_milliseconds());
  }
  std::string address = ip + ":" + std::to_string(GetPort());
  RAY_LOG(INFO) << "Gcs server address = " << address;

  RAY_CHECK_OK(GetOrConnectRedis()->GetPrimaryContext()->RunArgvAsync(
      {"SET", "GcsServerAddress", address}));
  RAY_LOG(INFO) << "Finished setting gcs server address: " << address;
}

void GcsServer::InitRaySyncer(const GcsInitData &gcs_init_data) {
  /*
    The current synchronization flow is:
        raylet -> syncer::poller --> syncer::update -> gcs_resource_manager
        gcs_placement_scheduler --/
  */
  ray_syncer_ = std::make_unique<syncer::RaySyncer>(
      main_service_, raylet_client_pool_, *gcs_resource_manager_);
  ray_syncer_->Initialize(gcs_init_data);
  ray_syncer_->Start();
}

void GcsServer::InitStatsHandler() {
  RAY_CHECK(gcs_table_storage_);
  stats_handler_.reset(new rpc::DefaultStatsHandler(gcs_table_storage_));
  // Register service.
  stats_service_.reset(new rpc::StatsGrpcService(main_service_, *stats_handler_));
  rpc_server_.RegisterService(*stats_service_);
}

void GcsServer::InitFunctionManager() {
  function_manager_ = std::make_unique<GcsFunctionManager>(kv_manager_->GetInstance());
}

void GcsServer::InitKVManager() {
  std::unique_ptr<InternalKVInterface> instance;
  // TODO (yic): Use a factory with configs
  if (storage_type_ == "redis") {
    instance = std::make_unique<RedisInternalKV>(GetRedisClientOptions());
  } else if (storage_type_ == "memory") {
    instance = std::make_unique<MemoryInternalKV>(main_service_);
  }

  kv_manager_ = std::make_unique<GcsInternalKVManager>(std::move(instance));
  kv_service_ = std::make_unique<rpc::InternalKVGrpcService>(main_service_, *kv_manager_);
  // Register service.
  rpc_server_.RegisterService(*kv_service_);
}

void GcsServer::InitPubSubHandler() {
  pubsub_handler_ =
      std::make_unique<InternalPubSubHandler>(pubsub_io_service_, gcs_publisher_);
  pubsub_service_ = std::make_unique<rpc::InternalPubSubGrpcService>(pubsub_io_service_,
                                                                     *pubsub_handler_);
  // Register service.
  rpc_server_.RegisterService(*pubsub_service_);
}

void GcsServer::InitRuntimeEnvManager() {
  runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
      /*deleter=*/[this](const std::string &plugin_uri, auto callback) {
        // A valid runtime env URI is of the form "plugin|protocol://hash".
        std::string plugin_sep = "|";
        std::string protocol_sep = "://";
        auto plugin_end_pos = plugin_uri.find(plugin_sep);
        auto protocol_end_pos = plugin_uri.find(protocol_sep);
        if (protocol_end_pos == std::string::npos ||
            plugin_end_pos == std::string::npos) {
          RAY_LOG(ERROR) << "Plugin URI must be of form "
                         << "<plugin>|<protocol>://<hash>, got " << plugin_uri;
          callback(false);
        } else {
          auto protocol_pos = plugin_end_pos + plugin_sep.size();
          int protocol_len = protocol_end_pos - protocol_pos;
          auto protocol = plugin_uri.substr(protocol_pos, protocol_len);
          if (protocol != "gcs") {
            // Some URIs do not correspond to files in the GCS.  Skip deletion for
            // these.
            callback(true);
          } else {
            auto uri = plugin_uri.substr(protocol_pos);
            this->kv_manager_->GetInstance().Del(
                "" /* namespace */,
                uri /* key */,
                false /* del_by_prefix*/,
                [callback = std::move(callback)](int64_t) { callback(false); });
          }
        }
      });
}

void GcsServer::InitGcsWorkerManager() {
  gcs_worker_manager_ =
      std::make_unique<GcsWorkerManager>(gcs_table_storage_, gcs_publisher_);
  // Register service.
  worker_info_service_.reset(
      new rpc::WorkerInfoGrpcService(main_service_, *gcs_worker_manager_));
  rpc_server_.RegisterService(*worker_info_service_);
}

void GcsServer::InstallEventListeners() {
  // Install node event listeners.
  gcs_node_manager_->AddNodeAddedListener([this](std::shared_ptr<rpc::GcsNodeInfo> node) {
    // Because a new node has been added, we need to try to schedule the pending
    // placement groups and the pending actors.
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_placement_group_manager_->OnNodeAdd(NodeID::FromBinary(node->node_id()));
    gcs_actor_manager_->SchedulePendingActors();
    gcs_heartbeat_manager_->AddNode(NodeID::FromBinary(node->node_id()));
    ray_syncer_->AddNode(*node);
  });
  gcs_node_manager_->AddNodeRemovedListener(
      [this](std::shared_ptr<rpc::GcsNodeInfo> node) {
        auto node_id = NodeID::FromBinary(node->node_id());
        const auto node_ip_address = node->node_manager_address();
        // All of the related placement groups and actors should be reconstructed when a
        // node is removed from the GCS.
        gcs_resource_manager_->OnNodeDead(node_id);
        gcs_placement_group_manager_->OnNodeDead(node_id);
        gcs_actor_manager_->OnNodeDead(node_id, node_ip_address);
        raylet_client_pool_->Disconnect(NodeID::FromBinary(node->node_id()));
        ray_syncer_->RemoveNode(*node);
      });

  // Install worker event listener.
  gcs_worker_manager_->AddWorkerDeadListener(
      [this](std::shared_ptr<rpc::WorkerTableData> worker_failure_data) {
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
                                         creation_task_exception);
      });

  // Install job event listeners.
  gcs_job_manager_->AddJobFinishedListener([this](std::shared_ptr<JobID> job_id) {
    gcs_actor_manager_->OnJobFinished(*job_id);
    gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(*job_id);
  });

  // Install scheduling event listeners.
  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    gcs_resource_manager_->AddResourcesChangedListener([this] {
      main_service_.post(
          [this] {
            // Because resources have been changed, we need to try to schedule the
            // pending actors.
            gcs_actor_manager_->SchedulePendingActors();
          },
          "GcsServer.SchedulePendingActors");
    });
  }
}

void GcsServer::RecordMetrics() const {
  gcs_actor_manager_->RecordMetrics();
  gcs_placement_group_manager_->RecordMetrics();
  execute_after(
      main_service_,
      [this] { RecordMetrics(); },
      (RayConfig::instance().metrics_report_interval_ms() / 2) /* milliseconds */);
}

void GcsServer::DumpDebugStateToFile() const {
  std::fstream fs;
  fs.open(config_.log_dir + "/debug_state_gcs.txt",
          std::fstream::out | std::fstream::trunc);
  fs << GetDebugState() << "\n\n";
  fs << main_service_.stats().StatsString();
  fs.close();
}

std::string GcsServer::GetDebugState() const {
  std::ostringstream stream;
  stream << gcs_node_manager_->DebugString() << "\n\n"
         << gcs_actor_manager_->DebugString() << "\n\n"
         << gcs_resource_manager_->DebugString() << "\n\n"
         << gcs_placement_group_manager_->DebugString() << "\n\n"
         << gcs_publisher_->DebugString() << "\n\n"
         << runtime_env_manager_->DebugString() << "\n\n";

  stream << ray_syncer_->DebugString();
  return stream.str();
}

std::shared_ptr<RedisClient> GcsServer::GetOrConnectRedis() {
  if (redis_client_ == nullptr) {
    redis_client_ = std::make_shared<RedisClient>(GetRedisClientOptions());
    auto status = redis_client_->Connect(main_service_);
    RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;

    // Init redis failure detector.
    gcs_redis_failure_detector_ = std::make_shared<GcsRedisFailureDetector>(
        main_service_, redis_client_->GetPrimaryContext(), [this]() { Stop(); });
    gcs_redis_failure_detector_->Start();
  }
  return redis_client_;
}

void GcsServer::PrintAsioStats() {
  /// If periodic asio stats print is enabled, it will print it.
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    RAY_LOG(INFO) << "Event stats:\n\n" << main_service_.stats().StatsString() << "\n\n";
  }
}

}  // namespace gcs
}  // namespace ray
