#include "gcs_server.h"
#include "actor_info_handler_impl.h"
#include "job_info_handler_impl.h"
#include "node_info_handler_impl.h"
#include "object_info_handler_impl.h"

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

  // Register rpc service.
  job_info_handler_ = InitJobInfoHandler();
  job_info_service_.reset(new rpc::JobInfoGrpcService(main_service_, *job_info_handler_));
  rpc_server_.RegisterService(*job_info_service_);

  actor_info_handler_ = InitActorInfoHandler();
  actor_info_service_.reset(
      new rpc::ActorInfoGrpcService(main_service_, *actor_info_handler_));
  rpc_server_.RegisterService(*actor_info_service_);

  node_info_handler_ = InitNodeInfoHandler();
  node_info_service_.reset(
      new rpc::NodeInfoGrpcService(main_service_, *node_info_handler_));
  rpc_server_.RegisterService(*node_info_service_);

  object_info_handler_ = InitObjectInfoHandler();
  object_info_service_.reset(
      new rpc::ObjectInfoGrpcService(main_service_, *object_info_handler_));
  rpc_server_.RegisterService(*object_info_service_);

  // Run rpc server.
  rpc_server_.Run();

  // Run the event loop.
  // Using boost::asio::io_context::work to avoid ending the event loop when
  // there are no events to handle.
  boost::asio::io_context::work worker(main_service_);
  main_service_.run();
}

void GcsServer::Stop() {
  // Shutdown the rpc server
  rpc_server_.Shutdown();

  // Stop the event loop.
  main_service_.stop();
}

void GcsServer::InitBackendClient() {
  GcsClientOptions options(config_.redis_address, config_.redis_port,
                           config_.redis_password, config_.is_test);
  redis_gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = redis_gcs_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
}

std::unique_ptr<rpc::JobInfoHandler> GcsServer::InitJobInfoHandler() {
  return std::unique_ptr<rpc::DefaultJobInfoHandler>(
      new rpc::DefaultJobInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::ActorInfoHandler> GcsServer::InitActorInfoHandler() {
  return std::unique_ptr<rpc::DefaultActorInfoHandler>(
      new rpc::DefaultActorInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::NodeInfoHandler> GcsServer::InitNodeInfoHandler() {
  return std::unique_ptr<rpc::DefaultNodeInfoHandler>(
      new rpc::DefaultNodeInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<rpc::ObjectInfoHandler> GcsServer::InitObjectInfoHandler() {
  return std::unique_ptr<rpc::DefaultObjectInfoHandler>(
      new rpc::DefaultObjectInfoHandler(*redis_gcs_client_));
}

}  // namespace gcs
}  // namespace ray
