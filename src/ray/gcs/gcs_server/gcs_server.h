#pragma once
#include <ray/gcs/redis_gcs_client.h>
#include <ray/rpc/gcs_server/job_info_access_server.h>
#include <ray/rpc/grpc_server.h>
#include <string>

namespace ray {
namespace gcs {

struct GcsServerConfig {
  std::string server_name = "GcsServer";
  uint16_t server_port = 0;
  uint16_t server_thread_num = 1;
  std::string redis_password;
  std::string redis_address;
  uint16_t redis_port = 6379;
  bool retry_redis = true;
};

class GcsServer {
 public:
  explicit GcsServer(const GcsServerConfig &config);
  virtual ~GcsServer();

  // Start gcs server
  void Start();

  // Stop gcs server
  void Stop();

  // Get the port of the gcs server is listening on
  int GetPort() const { return rpc_server_.GetPort(); }

 protected:
  // Initialize the backend storage client
  // The gcs server is just the proxy between the gcs client and reliable storage, so we
  // need a backend client to connect to the storage.
  virtual void InitBackendClient();

  // The job info access handler
  virtual std::unique_ptr<rpc::JobInfoAccessHandler> InitJobInfoAccessHandler();

 private:
  // Gcs server configuration
  GcsServerConfig config_;
  // The grpc server
  rpc::GrpcServer rpc_server_;
  // The main io service to drive event posted from grpc threads.
  boost::asio::io_context main_service_;
  // Job info access handler and service
  std::unique_ptr<rpc::JobInfoAccessHandler> job_info_access_handler_;
  std::unique_ptr<rpc::JobInfoAccessGrpcService> job_info_access_service_;
  // Backend client
  std::shared_ptr<RedisGcsClient> redis_gcs_client_;
};

}  // namespace gcs
}  // namespace ray
