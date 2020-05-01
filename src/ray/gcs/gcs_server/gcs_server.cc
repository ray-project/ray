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
                  config.grpc_server_thread_num) {}

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
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(
      main_service_, redis_gcs_client_->Actors(), *gcs_node_manager_);
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
