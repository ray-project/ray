#ifndef RAY_GCS_GCS_SERVER_H
#define RAY_GCS_GCS_SERVER_H

#include <ray/gcs/redis_gcs_client.h>
#include <ray/rpc/gcs_server/gcs_rpc_server.h>
#include <ray/rpc/grpc_server.h>
#include <string>

namespace ray {
namespace gcs {

struct GcsServerConfig {
  std::string grpc_server_name = "GcsServer";
  uint16_t grpc_server_port = 0;
  uint16_t grpc_server_thread_num = 1;
  std::string redis_password;
  std::string redis_address;
  uint16_t redis_port = 6379;
  bool retry_redis = true;
  bool is_test = false;
};

/// The GcsServer will take over all requests from ServiceBasedGcsClient and transparent
/// transmit the command to the backend reliable storage for the time being.
/// In the future, GCS server's main responsibility is to manage meta data
/// and the management of actor creation.
/// For more details, please see the design document.
/// https://docs.google.com/document/d/1d-9qBlsh2UQHo-AWMWR0GptI_Ajwu4SKx0Q0LHKPpeI/edit#heading=h.csi0gaglj2pv
class GcsServer {
 public:
  explicit GcsServer(const GcsServerConfig &config);
  virtual ~GcsServer();

  /// Start gcs server.
  void Start();

  /// Stop gcs server.
  void Stop();

  /// Get the port of this gcs server.
  int GetPort() const { return rpc_server_.GetPort(); }

 protected:
  /// Initialize the backend storage client
  /// The gcs server is just the proxy between the gcs client and reliable storage
  /// for the time being, so we need a backend client to connect to the storage.
  virtual void InitBackendClient();

  /// The job info handler
  virtual std::unique_ptr<rpc::JobInfoHandler> InitJobInfoHandler();

  /// The actor info handler
  virtual std::unique_ptr<rpc::ActorInfoHandler> InitActorInfoHandler();

 private:
  /// Gcs server configuration
  GcsServerConfig config_;
  /// The grpc server
  rpc::GrpcServer rpc_server_;
  /// The main io service to drive event posted from grpc threads.
  boost::asio::io_context main_service_;
  /// Job info handler and service
  std::unique_ptr<rpc::JobInfoHandler> job_info_handler_;
  std::unique_ptr<rpc::JobInfoGrpcService> job_info_service_;
  /// Actor info handler and service
  std::unique_ptr<rpc::ActorInfoHandler> actor_info_handler_;
  std::unique_ptr<rpc::ActorInfoGrpcService> actor_info_service_;
  /// Backend client
  std::shared_ptr<RedisGcsClient> redis_gcs_client_;
};

}  // namespace gcs
}  // namespace ray

#endif
