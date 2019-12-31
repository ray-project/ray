#ifndef RAY_GCS_SERVICE_BASED_GCS_CLIENT_H
#define RAY_GCS_SERVICE_BASED_GCS_CLIENT_H

#include <map>
#include <string>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/asio.h"
#include "ray/gcs/gcs_client.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"
#include "src/ray/rpc/client_call.h"

namespace ray {

namespace gcs {

class RAY_EXPORT ServiceBasedGcsClient : public GcsClient {
 public:
  /// Constructor of ServiceBasedGcsClient.
  /// Connect() must be called(and return ok) before you call any other methods.
  ///
  /// \param options Options of this client, e.g. server address, password and so on.
  ServiceBasedGcsClient(const GcsClientOptions &options);

  /// Connect to GCS Service. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_service The event loop for this client.
  /// Must be single-threaded io_service.
  ///
  /// \return Status
  Status Connect(boost::asio::io_service &io_service);

  /// Disconnect with GCS Service. Non-thread safe.
  void Disconnect();

  RedisGcsClient &GetRedisGcsClient() { return *redis_gcs_client_; }

  rpc::GcsRpcClient &GetGcsRpcClient() { return *gcs_rpc_client_; }

 private:
  std::unique_ptr<RedisGcsClient> redis_gcs_client_;

  // Gcs rpc client
  std::unique_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SERVICE_BASED_GCS_CLIENT_H
