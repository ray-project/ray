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
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"
#include "ray/gcs/gcs_server/gcs_worker_manager.h"
#include "ray/gcs/gcs_server/runtime_env_handler.h"
#include "ray/gcs/gcs_server/store_client_kv.h"
#include "ray/gcs/store_client/observable_store_client.h"
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
      local_node_id_(NodeID::FromRandom()),
      pubsub_periodical_runner_(pubsub_io_service_),
      periodical_runner_(main_service),
      is_started_(false),
      is_stopped_(false) {
  // Init GCS table storage.
  RAY_LOG(INFO) << "GCS storage type is " << storage_type_;
  if (storage_type_ == "redis") {
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(GetOrConnectRedis());
  } else if (storage_type_ == "memory") {
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
  // Init grpc based pubsub on GCS.
  // TODO: Move this into GcsPublisher.
  inner_publisher = std::make_unique<pubsub::Publisher>(
      /*channels=*/
      std::vector<rpc::ChannelType>{
          rpc::ChannelType::GCS_ACTOR_CHANNEL,
          rpc::ChannelType::GCS_JOB_CHANNEL,
          rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
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

  gcs_publisher_ = std::make_shared<GcsPublisher>(std::move(inner_publisher));
}

GcsServer::~GcsServer() { Stop(); }

RedisClientOptions GcsServer::GetRedisClientOptions() const {
  return RedisClientOptions(config_.redis_address,
                            config_.redis_port,
                            config_.redis_password,
                            config_.enable_sharding_conn,
                            config_.enable_redis_ssl);
}

void GcsServer::Start() {
  // Load gcs tables data asynchronously.
  auto gcs_init_data = std::make_shared<GcsInitData>(gcs_table_storage_);
  gcs_init_data->AsyncLoad([this, gcs_init_data] { DoStart(*gcs_init_data); });
}

void GcsServer::DoStart(const GcsInitData &gcs_init_data) {
  // Init cluster resource scheduler.
  InitClusterResourceScheduler();

  // Init cluster task manager.
  InitClusterTaskManager();

  // Init gcs resource manager.
  InitGcsResourceManager(gcs_init_data);

  // Init synchronization service
  InitRaySyncer(gcs_init_data);

  // Init gcs node manager.
  InitGcsNodeManager(gcs_init_data);

  // Init gcs health check manager.
  InitGcsHealthCheckManager(gcs_init_data);

  // Init KV Manager
  InitKVManager();

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

  // Start RPC server when all tables have finished loading initial
  // data.
  rpc_server_.Run();

  // Init usage stats client
  // This is done after the RPC server starts
  // since we need to know the port the rpc server listens on.
  InitUsageStatsClient();
  gcs_worker_manager_->SetUsageStatsClient(usage_stats_client_.get());
  gcs_actor_manager_->SetUsageStatsClient(usage_stats_client_.get());
  gcs_placement_group_manager_->SetUsageStatsClient(usage_stats_client_.get());

  RecordMetrics();

  periodical_runner_.RunFnPeriodically(
      [this] {
        RAY_LOG(INFO) << GetDebugState();
        PrintAsioStats();
      },
      /*ms*/ RayConfig::instance().event_stats_print_interval_ms(),
      "GCSServer.deadline_timer.debug_state_event_stats_print");

  global_gc_throttler_ =
      std::make_unique<Throttler>(RayConfig::instance().global_gc_min_interval_s() * 1e9);

  periodical_runner_.RunFnPeriodically(
      [this] {
        DumpDebugStateToFile();
        TryGlobalGC();
      },
      /*ms*/ RayConfig::instance().debug_dump_period_milliseconds(),
      "GCSServer.deadline_timer.debug_state_dump");

  is_started_ = true;
}

void GcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";
    if (RayConfig::instance().use_ray_syncer()) {
      ray_syncer_io_context_.stop();
      ray_syncer_thread_->join();
      ray_syncer_.reset();
    } else {
      gcs_ray_syncer_->Stop();
    }

    gcs_task_manager_->Stop();

    // Shutdown the rpc server
    rpc_server_.Shutdown();

    pubsub_handler_->Stop();
    kv_manager_.reset();

    is_stopped_ = true;
    gcs_redis_failure_detector_.reset();
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

void GcsServer::InitGcsHealthCheckManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);
  auto node_death_callback = [this](const NodeID &node_id) {
    main_service_.post(
        [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id); },
        "GcsServer.NodeDeathCallback");
  };

  gcs_healthcheck_manager_ =
      std::make_unique<GcsHealthCheckManager>(main_service_, node_death_callback);
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
  gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
      main_service_,
      cluster_resource_scheduler_->GetClusterResourceManager(),
      local_node_id_,
      cluster_task_manager_);

  // Initialize by gcs tables data.
  gcs_resource_manager_->Initialize(gcs_init_data);
  // Register service.
  node_resource_info_service_.reset(
      new rpc::NodeResourceInfoGrpcService(main_service_, *gcs_resource_manager_));
  rpc_server_.RegisterService(*node_resource_info_service_);

  periodical_runner_.RunFnPeriodically(
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
            raylet_client->GetResourceLoad([this](auto &status, auto &load) {
              if (status.ok()) {
                gcs_resource_manager_->UpdateResourceLoads(load.resources());
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
      scheduling::NodeID(local_node_id_.Binary()),
      NodeResources(),
      /*is_node_available_fn=*/
      [](auto) { return true; },
      /*is_local_node_with_raylet=*/false);
}

void GcsServer::InitClusterTaskManager() {
  RAY_CHECK(cluster_resource_scheduler_);
  cluster_task_manager_ = std::make_shared<ClusterTaskManager>(
      local_node_id_,
      cluster_resource_scheduler_,
      /*get_node_info=*/
      [this](const NodeID &node_id) {
        auto node = gcs_node_manager_->GetAliveNode(node_id);
        return node.has_value() ? node.value().get() : nullptr;
      },
      /*announce_infeasible_task=*/
      nullptr,
      /*local_task_manager=*/
      std::make_shared<NoopLocalTaskManager>());
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

  RAY_CHECK(gcs_resource_manager_ && cluster_task_manager_);
  scheduler = std::make_unique<GcsActorScheduler>(
      main_service_,
      gcs_table_storage_->ActorTable(),
      *gcs_node_manager_,
      cluster_task_manager_,
      schedule_failure_handler,
      schedule_success_handler,
      raylet_client_pool_,
      client_factory,
      /*normal_task_resources_changed_callback=*/
      [this](const NodeID &node_id, const rpc::ResourcesData &resources) {
        gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, resources);
      });
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(
      std::move(scheduler),
      gcs_table_storage_,
      gcs_publisher_,
      *runtime_env_manager_,
      *function_manager_,
      [this](const ActorID &actor_id) {
        gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
      },
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
  gcs_placement_group_scheduler_ =
      std::make_shared<GcsPlacementGroupScheduler>(main_service_,
                                                   gcs_table_storage_,
                                                   *gcs_node_manager_,
                                                   *cluster_resource_scheduler_,
                                                   raylet_client_pool_,
                                                   gcs_ray_syncer_.get());

  gcs_placement_group_manager_ = std::make_shared<GcsPlacementGroupManager>(
      main_service_,
      gcs_placement_group_scheduler_,
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
      RAY_LOG(INFO) << "Using external Redis for KV storage: " << config_.redis_address
                    << ":" << config_.redis_port;
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

void GcsServer::InitRaySyncer(const GcsInitData &gcs_init_data) {
  if (RayConfig::instance().use_ray_syncer()) {
    ray_syncer_ = std::make_unique<syncer::RaySyncer>(ray_syncer_io_context_,
                                                      local_node_id_.Binary());
    ray_syncer_->Register(
        syncer::MessageType::RESOURCE_VIEW, nullptr, gcs_resource_manager_.get());
    ray_syncer_->Register(
        syncer::MessageType::COMMANDS, nullptr, gcs_resource_manager_.get());
    ray_syncer_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::io_service::work work(ray_syncer_io_context_);
      ray_syncer_io_context_.run();
    });

    for (const auto &pair : gcs_init_data.Nodes()) {
      if (pair.second.state() ==
          rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_ALIVE) {
        rpc::Address address;
        address.set_raylet_id(pair.second.node_id());
        address.set_ip_address(pair.second.node_manager_address());
        address.set_port(pair.second.node_manager_port());

        auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(address);
        ray_syncer_->Connect(raylet_client->GetChannel());
      }
    }
  } else {
    /*
      The current synchronization flow is:
      raylet -> syncer::poller --> syncer::update -> gcs_resource_manager
      gcs_placement_scheduler --/
    */
    gcs_ray_syncer_ = std::make_unique<gcs_syncer::RaySyncer>(
        main_service_, raylet_client_pool_, *gcs_resource_manager_);
    gcs_ray_syncer_->Initialize(gcs_init_data);
    gcs_ray_syncer_->Start();
  }
}

void GcsServer::InitFunctionManager() {
  function_manager_ = std::make_unique<GcsFunctionManager>(kv_manager_->GetInstance());
}

void GcsServer::InitUsageStatsClient() {
  usage_stats_client_ = std::make_unique<UsageStatsClient>(
      "127.0.0.1:" + std::to_string(GetPort()), main_service_);
}

void GcsServer::InitKVManager() {
  std::unique_ptr<InternalKVInterface> instance;
  // TODO (yic): Use a factory with configs
  if (storage_type_ == "redis") {
    instance = std::make_unique<StoreClientInternalKV>(
        std::make_shared<RedisStoreClient>(GetOrConnectRedis()));
  } else if (storage_type_ == "memory") {
    instance =
        std::make_unique<StoreClientInternalKV>(std::make_unique<ObservableStoreClient>(
            std::make_unique<InMemoryStoreClient>(main_service_)));
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
        // A valid runtime env URI is of the form "protocol://hash".
        std::string protocol_sep = "://";
        auto protocol_end_pos = plugin_uri.find(protocol_sep);
        if (protocol_end_pos == std::string::npos) {
          RAY_LOG(ERROR) << "Plugin URI must be of form "
                         << "<protocol>://<hash>, got " << plugin_uri;
          callback(false);
        } else {
          auto protocol = plugin_uri.substr(0, protocol_end_pos);
          if (protocol != "gcs") {
            // Some URIs do not correspond to files in the GCS.  Skip deletion for
            // these.
            callback(true);
          } else {
            this->kv_manager_->GetInstance().Del(
                "" /* namespace */,
                plugin_uri /* key */,
                false /* del_by_prefix*/,
                [callback = std::move(callback)](int64_t) { callback(false); });
          }
        }
      });
  runtime_env_handler_ = std::make_unique<RuntimeEnvHandler>(
      main_service_,
      *runtime_env_manager_, /*delay_executor=*/
      [this](std::function<void()> task, uint32_t delay_ms) {
        return execute_after(main_service_, task, delay_ms);
      });
  runtime_env_service_ =
      std::make_unique<rpc::RuntimeEnvGrpcService>(main_service_, *runtime_env_handler_);
  // Register service.
  rpc_server_.RegisterService(*runtime_env_service_);
}

void GcsServer::InitGcsWorkerManager() {
  gcs_worker_manager_ =
      std::make_unique<GcsWorkerManager>(gcs_table_storage_, gcs_publisher_);
  // Register service.
  worker_info_service_.reset(
      new rpc::WorkerInfoGrpcService(main_service_, *gcs_worker_manager_));
  rpc_server_.RegisterService(*worker_info_service_);
}

void GcsServer::InitGcsTaskManager() {
  gcs_task_manager_ = std::make_unique<GcsTaskManager>();
  // Register service.
  task_info_service_.reset(new rpc::TaskInfoGrpcService(gcs_task_manager_->GetIoContext(),
                                                        *gcs_task_manager_));
  rpc_server_.RegisterService(*task_info_service_);
}

void GcsServer::InstallEventListeners() {
  // Install node event listeners.
  gcs_node_manager_->AddNodeAddedListener([this](std::shared_ptr<rpc::GcsNodeInfo> node) {
    // Because a new node has been added, we need to try to schedule the pending
    // placement groups and the pending actors.
    auto node_id = NodeID::FromBinary(node->node_id());
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_placement_group_manager_->OnNodeAdd(node_id);
    gcs_actor_manager_->SchedulePendingActors();
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

    if (RayConfig::instance().use_ray_syncer()) {
      ray_syncer_->Connect(raylet_client->GetChannel());
    } else {
      gcs_ray_syncer_->AddNode(*node);
    }
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
        raylet_client_pool_->Disconnect(node_id);
        gcs_healthcheck_manager_->RemoveNode(node_id);

        if (RayConfig::instance().use_ray_syncer()) {
          ray_syncer_->Disconnect(node_id.Binary());
        } else {
          gcs_ray_syncer_->RemoveNode(*node);
        }
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
                                         worker_failure_data->exit_detail(),
                                         creation_task_exception);
        gcs_placement_group_scheduler_->HandleWaitingRemovedBundles();
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
            // pending placement groups and actors.
            gcs_placement_group_manager_->SchedulePendingPlacementGroups();
            cluster_task_manager_->ScheduleAndDispatchTasks();
          },
          "GcsServer.SchedulePendingActors");
    });

    gcs_placement_group_scheduler_->AddResourcesChangedListener([this] {
      main_service_.post(
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
         << runtime_env_manager_->DebugString() << "\n\n"
         << gcs_task_manager_->DebugString() << "\n\n";
  if (gcs_ray_syncer_) {
    stream << gcs_ray_syncer_->DebugString();
  }
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
    rpc::ResourcesData resources_data;
    resources_data.set_should_global_gc(true);

    if (RayConfig::instance().use_ray_syncer()) {
      auto msg = std::make_shared<syncer::RaySyncMessage>();
      msg->set_version(absl::GetCurrentTimeNanos());
      msg->set_node_id(local_node_id_.Binary());
      msg->set_message_type(syncer::MessageType::COMMANDS);
      std::string serialized_msg;
      RAY_CHECK(resources_data.SerializeToString(&serialized_msg));
      msg->set_sync_message(std::move(serialized_msg));
      ray_syncer_->BroadcastRaySyncMessage(std::move(msg));
    } else {
      resources_data.set_node_id(local_node_id_.Binary());
      gcs_ray_syncer_->Update(resources_data);
    }

    global_gc_throttler_->RunNow();
  }
}

}  // namespace gcs
}  // namespace ray
