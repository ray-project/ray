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

#pragma once

#include "ray/gcs/gcs_client.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
namespace gcs {

class RAY_EXPORT ServiceBasedGcsClient : public GcsClient {
 public:
  explicit ServiceBasedGcsClient(const GcsClientOptions &options);

  Status Connect(boost::asio::io_service &io_service) override;

  void Disconnect() override;

  GcsPubSub &GetGcsPubSub() { return *gcs_pub_sub_; }

  rpc::GcsRpcClient &GetGcsRpcClient() { return *gcs_rpc_client_; }

 private:
  /// Get gcs server address from redis.
  /// This address is set by GcsServer::StoreGcsServerAddressInRedis function.
  ///
  /// \param context The context of redis.
  /// \param address The address of gcs server.
  /// \param max_attempts The maximum number of times to get gcs server rpc address.
  /// \return Returns true if gcs server address is obtained, False otherwise.
  bool GetGcsServerAddressFromRedis(redisContext *context,
                                    std::pair<std::string, int> *address,
                                    int max_attempts = 1);

  /// Fire a periodic timer to check if GCS sever address has changed.
  void PeriodicallyCheckGcsServerAddress();

  /// This function is used to redo subscription and reconnect to GCS RPC server when gcs
  /// service failure is detected.
  ///
  /// \param type The type of GCS service failure.
  void GcsServiceFailureDetected(rpc::GcsServiceFailureType type);

  /// Reconnect to GCS RPC server.
  void ReconnectGcsServer();

  std::shared_ptr<RedisClient> redis_client_;

  std::unique_ptr<GcsPubSub> gcs_pub_sub_;

  // Gcs rpc client
  std::unique_ptr<rpc::GcsRpcClient> gcs_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // A timer used to check if gcs server address changed.
  std::unique_ptr<boost::asio::deadline_timer> detect_timer_;
  std::function<bool(std::pair<std::string, int> *)> get_server_address_func_;
  std::function<void(bool)> resubscribe_func_;
  std::pair<std::string, int> current_gcs_server_address_;
  int64_t last_reconnect_timestamp_ms_;
  std::pair<std::string, int> last_reconnect_address_;
};

}  // namespace gcs
}  // namespace ray
