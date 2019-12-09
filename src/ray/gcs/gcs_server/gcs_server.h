#pragma once
#include <ray/gcs/redis_gcs_client.h>
#include <ray/rpc/gcs_server/job_info_access_server.h>
#include <ray/rpc/grpc_server.h>
#include <string>

namespace ray {
namespace gcs {

struct GcsServerConfig {
  std::string server_name;
  uint16_t server_port;
  uint16_t server_thread_num;
  std::string redis_password;
  std::string redis_address;
  uint16_t redis_port;
  bool retry_redis;
};

class GcsServer {
 public:
  explicit GcsServer(const GcsServerConfig &config);

  virtual ~GcsServer();

  void Start();

  void Stop();

  int GetPort() const { return rpc_server_.GetPort(); }

 protected:
  virtual void InitBackendClient();
  virtual std::unique_ptr<rpc::JobInfoAccessHandler> InitJobInfoAccessHandler();

 private:
  GcsServerConfig config_;
  rpc::GrpcServer rpc_server_;
  boost::asio::io_context main_service_;

  // Job info access
  std::unique_ptr<rpc::JobInfoAccessHandler> job_info_access_handler_;
  std::unique_ptr<rpc::JobInfoAccessGrpcService> job_info_access_service_;

  std::shared_ptr<RedisGcsClient> redis_gcs_client_;
};

}  // namespace gcs
}  // namespace ray