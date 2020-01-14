#ifndef RAY_GCS_SERVICE_BASED_GCS_CLIENT_H
#define RAY_GCS_SERVICE_BASED_GCS_CLIENT_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
namespace gcs {

class RAY_EXPORT ServiceBasedGcsClient : public GcsClient {
 public:
  ServiceBasedGcsClient(const GcsClientOptions &options);

  ServiceBasedGcsClient(RedisGcsClient *redis_gcs_client);

  Status Connect(boost::asio::io_service &io_service) override;

  void Disconnect() override;

  RedisGcsClient &GetRedisGcsClient() { return *redis_gcs_client_; }

  rpc::GcsRpcClient &GetGcsRpcClient() { return *gcs_rpc_client_; }

  std::string DebugString() const override;

 private:
  std::unique_ptr<RedisGcsClient> redis_gcs_client_;

  // Gcs rpc client
  std::unique_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_SERVICE_BASED_GCS_CLIENT_H
