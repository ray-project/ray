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

#include "gcs_server.h"
#include "actor_info_handler_impl.h"
#include "error_info_handler_impl.h"
#include "gcs_actor_manager.h"
#include "gcs_node_manager.h"
#include "job_info_handler_impl.h"
#include "object_info_handler_impl.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "stats_handler_impl.h"
#include "task_info_handler_impl.h"
#include "worker_info_handler_impl.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config)
    : config_(config),
      rpc_server_(config.grpc_server_name, config.grpc_server_port,
                  config.grpc_server_thread_num),
      client_call_manager_(main_service_) {}

GcsServer::~GcsServer() { Stop(); }

void GcsServer::Start() {
  // Init backend client.
  InitBackendClient();

  // Init gcs node_manager.
  InitGcsNodeManager();

  // Init gcs pub sub instance.
  gcs_pub_sub_ = std::make_shared<gcs::GcsPubSub>(redis_gcs_client_->GetRedisClient());

  // Init gcs detector.
  gcs_redis_failure_detector_ = std::make_shared<GcsRedisFailureDetector>(
      main_service_, redis_gcs_client_->primary_context(), [this]() { Stop(); });
  gcs_redis_failure_detector_->Start();

  // Init gcs actor manager.
  InitGcsActorManager();

  // Register rpc service.
  job_info_handler_ = InitJobInfoHandler();
  job_info_service_.reset(new rpc::JobInfoGrpcService(main_service_, *job_info_handler_));
  rpc_server_.RegisterService(*job_info_service_);

  actor_info_handler_ = InitActorInfoHandler();
  actor_info_service_.reset(
      new rpc::ActorInfoGrpcService(main_service_, *actor_info_handler_));
  rpc_server_.RegisterService(*actor_info_service_);

  node_info_service_.reset(
      new rpc::NodeInfoGrpcService(main_service_, *gcs_node_manager_));
  rpc_server_.RegisterService(*node_info_service_);

  object_info_handler_ = InitObjectInfoHandler();
  object_info_service_.reset(
      new rpc::ObjectInfoGrpcService(main_service_, *object_info_handler_));
  rpc_server_.RegisterService(*object_info_service_);

  task_info_handler_ = InitTaskInfoHandler();
  task_info_service_.reset(
      new rpc::TaskInfoGrpcService(main_service_, *task_info_handler_));
  rpc_server_.RegisterService(*task_info_service_);

  stats_handler_ = InitStatsHandler();
  stats_service_.reset(new rpc::StatsGrpcService(main_service_, *stats_handler_));
  rpc_server_.RegisterService(*stats_service_);

  error_info_handler_ = InitErrorInfoHandler();
  error_info_service_.reset(
      new rpc::ErrorInfoGrpcService(main_service_, *error_info_handler_));
  rpc_server_.RegisterService(*error_info_service_);

  worker_info_handler_ = InitWorkerInfoHandler();
  worker_info_service_.reset(
      new rpc::WorkerInfoGrpcService(main_service_, *worker_info_handler_));
  rpc_server_.RegisterService(*worker_info_service_);

  // Run rpc server.
  rpc_server_.Run();

  // Store gcs rpc server address in redis.
  StoreGcsServerAddressInRedis();
  is_started_ = true;

  // Run the event loop.
  // Using boost::asio::io_context::work to avoid ending the event loop when
  // there are no events to handle.
  boost::asio::io_context::work worker(main_service_);
  main_service_.run();
}

void GcsServer::Stop() {
  RAY_LOG(INFO) << "Stopping gcs server.";
  // Shutdown the rpc server
  rpc_server_.Shutdown();

  // Stop the event loop.
  main_service_.stop();

  is_stopped_ = true;
  RAY_LOG(INFO) << "Finished stopping gcs server.";
}

void GcsServer::InitBackendClient() {
  GcsClientOptions options(config_.redis_address, config_.redis_port,
                           config_.redis_password, config_.is_test);
  redis_gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = redis_gcs_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
}

void GcsServer::InitGcsNodeManager() {
  RAY_CHECK(redis_gcs_client_ != nullptr);
  gcs_node_manager_ = std::make_shared<GcsNodeManager>(
      main_service_, redis_gcs_client_->Nodes(), redis_gcs_client_->Errors());
}

void GcsServer::InitGcsActorManager() {
  RAY_CHECK(redis_gcs_client_ != nullptr && gcs_node_manager_ != nullptr);
  auto scheduler = std::make_shared<GcsActorScheduler>(
      main_service_, redis_gcs_client_->Actors(), *gcs_node_manager_,
      /*schedule_failure_handler=*/
      [this](std::shared_ptr<GcsActor> actor) {
        // When there are no available nodes to schedule the actor the
        // gcs_actor_scheduler will treat it as failed and invoke this handler. In
        // this case, the actor manager should schedule the actor once an
        // eligible node is registered.
        gcs_actor_manager_->OnActorCreationFailed(std::move(actor));
      },
      /*schedule_success_handler=*/
      [this](std::shared_ptr<GcsActor> actor) {
        gcs_actor_manager_->OnActorCreationSuccess(std::move(actor));
      },
      /*lease_client_factory=*/
      [this](const rpc::Address &address) {
        auto node_manager_worker_client = rpc::NodeManagerWorkerClient::make(
            address.ip_address(), address.port(), client_call_manager_);
        return std::make_shared<ray::raylet::RayletClient>(
            std::move(node_manager_worker_client));
      },
      /*client_factory=*/
      [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      });
  gcs_actor_manager_ =
      std::make_shared<GcsActorManager>(scheduler, redis_gcs_client_->Actors());
  gcs_node_manager_->AddNodeAddedListener(
      [this](const std::shared_ptr<rpc::GcsNodeInfo> &) {
        // Because a new node has been added, we need to try to schedule the pending
        // actors.
        gcs_actor_manager_->SchedulePendingActors();
      });

  gcs_node_manager_->AddNodeRemovedListener([this](
                                                std::shared_ptr<rpc::GcsNodeInfo> node) {
    // All of the related actors should be reconstructed when a node is removed from the
    // GCS.
    gcs_actor_manager_->ReconstructActorsOnNode(ClientID::FromBinary(node->node_id()));
  });
  RAY_CHECK_OK(redis_gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
      [this](const WorkerID &id, const rpc::WorkerFailureData &worker_failure_data) {
        auto &worker_address = worker_failure_data.worker_address();
        WorkerID worker_id = WorkerID::FromBinary(worker_address.worker_id());
        ClientID node_id = ClientID::FromBinary(worker_address.raylet_id());
        auto needs_restart = !worker_failure_data.intentional_disconnect();
        gcs_actor_manager_->ReconstructActorOnWorker(node_id, worker_id, needs_restart);
      },
      /*done_callback=*/nullptr));
}

std::unique_ptr<rpc::JobInfoHandler> GcsServer::InitJobInfoHandler() {
  return std::unique_ptr<rpc::DefaultJobInfoHandler>(
      new rpc::DefaultJobInfoHandler(*redis_gcs_client_, gcs_pub_sub_));
}

std::unique_ptr<rpc::ActorInfoHandler> GcsServer::InitActorInfoHandler() {
  return std::unique_ptr<rpc::DefaultActorInfoHandler>(
      new rpc::DefaultActorInfoHandler(*redis_gcs_client_, *gcs_actor_manager_));
}

std::unique_ptr<rpc::ObjectInfoHandler> GcsServer::InitObjectInfoHandler() {
  return std::unique_ptr<rpc::DefaultObjectInfoHandler>(
      new rpc::DefaultObjectInfoHandler(*redis_gcs_client_));
}

void GcsServer::StoreGcsServerAddressInRedis() {
  std::string address =
      GetValidLocalIp(
          GetPort(),
          RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()) +
      ":" + std::to_string(GetPort());
  RAY_LOG(INFO) << "Gcs server address = " << address;

  RAY_CHECK_OK(redis_gcs_client_->primary_context()->RunArgvAsync(
      {"SET", "GcsServerAddress", address}));
  RAY_LOG(INFO) << "Finished setting gcs server address: " << address;
}

std::unique_ptr<rpc::TaskInfoHandler> GcsServer::InitTaskInfoHandler() {
  return std::unique_ptr<rpc::DefaultTaskInfoHandler>(
      new rpc::DefaultTaskInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::StatsHandler> GcsServer::InitStatsHandler() {
  return std::unique_ptr<rpc::DefaultStatsHandler>(
      new rpc::DefaultStatsHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::ErrorInfoHandler> GcsServer::InitErrorInfoHandler() {
  return std::unique_ptr<rpc::DefaultErrorInfoHandler>(
      new rpc::DefaultErrorInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::WorkerInfoHandler> GcsServer::InitWorkerInfoHandler() {
  return std::unique_ptr<rpc::DefaultWorkerInfoHandler>(new rpc::DefaultWorkerInfoHandler(
      *redis_gcs_client_, *gcs_actor_manager_, gcs_pub_sub_));
}

}  // namespace gcs
}  // namespace ray
