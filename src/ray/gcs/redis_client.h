#ifndef RAY_GCS_REDIS_CLIENT_H
#define RAY_GCS_REDIS_CLIENT_H

#include <map>
#include <string>

#include "ray/common/status.h"
#include "ray/gcs/asio.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisContext;

class RedisClientOptions {
 public:
  RedisClientOptions(const std::string &ip, int port, const std::string &password,
                     bool is_test_client = false)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        is_test_client_(is_test_client) {}

  // Redis server address
  std::string server_ip_;
  int server_port_;

  // Password of Redis.
  std::string password_;

  // Whether this client is used for tests.
  bool is_test_client_{false};
};

/// \class RedisClient
/// Implementation of
class RedisClient {
 public:
  RedisClient(const RedisClientOptions &options);

  virtual ~RedisClient();

  /// Connect to Redis. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_service The event loop for this client.
  /// Must be single-threaded io_service (get more information from RedisAsioClient).
  /// \return Status
  Status Connect(boost::asio::io_service &io_service);

  // TODO(micafan) Maybe it's not necessary to use multi threads.
  /// Connect to Redis. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_services The event loops for this client. Each RedisContext bind to
  // a event loop.
  /// Must be single-threaded io_service (get more information from RedisAsioClient).
  /// \return Status
  Status Connect(std::vector<boost::asio::io_service *> io_services);

  /// Disconnect with Redis. Non-thread safe.
  void Disconnect();

  std::vector<std::shared_ptr<RedisContext>> GetShardContexts() {
    return shard_contexts_;
  }

  std::shared_ptr<RedisContext> GetRedisContext(const std::string &shard_key);

  std::shared_ptr<RedisContext> GetPrimaryContext() { return primary_context_; }

 protected:
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  void Attach();

  RedisClientOptions options_;

  /// Whether this client is connected to redis.
  bool is_connected_{false};

  // The following contexts write to the data shard
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_async_clients_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_subscribe_clients_;
  // The following context writes everything to the primary shard
  std::shared_ptr<RedisContext> primary_context_;
  std::unique_ptr<RedisAsioClient> asio_async_auxiliary_client_;
  std::unique_ptr<RedisAsioClient> asio_subscribe_auxiliary_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_CLIENT_H