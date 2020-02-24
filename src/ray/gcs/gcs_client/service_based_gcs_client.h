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

  /// This function is thread safe.
  uint64_t Reconnect(uint64_t reconnect_count);

  RedisGcsClient &GetRedisGcsClient() { return *redis_gcs_client_; }

  rpc::GcsRpcClient &GetGcsRpcClient() { return *gcs_rpc_client_; }

 private:
  /// Get gcs server address from redis.
  /// This address is set by GcsServer::StoreGcsServerAddressInRedis function.
  ///
  /// \param context The context of redis.
  /// \param address The address of gcs server.
  void GetGcsServerAddressFromRedis(redisContext *context,
                                    std::pair<std::string, int> *address);

  std::unique_ptr<RedisGcsClient> redis_gcs_client_;

  // Gcs rpc client
  std::unique_ptr<rpc::GcsRpcClient> gcs_rpc_client_ GUARDED_BY(mutex_);
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // Gcs server address
  std::pair<std::string, int> address_ GUARDED_BY(mutex_);

  // Mutex to protect the gcs_rpc_client_ field.
  absl::Mutex mutex_;

  uint64_t reconnect_count_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_SERVICE_BASED_GCS_CLIENT_H
