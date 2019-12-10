#include "gcs_server.h"
#include "default_job_info_access_handler.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config)
    : config_(config),
      rpc_server_(config.server_name, config.server_port, config.server_thread_num) {}

GcsServer::~GcsServer() { Stop(); }

void GcsServer::Start() {
  // Init redis gcs client
  InitBackendClient();

  // Register rpc service
  job_info_access_handler_ = InitJobInfoAccessHandler();
  job_info_access_service_.reset(
      new rpc::JobInfoAccessGrpcService(main_service_, *job_info_access_handler_));
  rpc_server_.RegisterService(*job_info_access_service_);

  rpc_server_.Run();

  boost::asio::io_context::work worker(main_service_);
  main_service_.run();
}

void GcsServer::Stop() {
  // Shutdown the rpc server
  rpc_server_.Shutdown();

  // Stop the io context
  main_service_.stop();
}

void GcsServer::InitBackendClient() {
  GcsClientOptions options(config_.redis_address, config_.redis_port,
                           config_.redis_password);
  redis_gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = redis_gcs_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
}

std::unique_ptr<rpc::JobInfoAccessHandler> GcsServer::InitJobInfoAccessHandler() {
  return std::unique_ptr<rpc::DefaultJobInfoAccessHandler>();
}

}  // namespace gcs
}  // namespace ray
