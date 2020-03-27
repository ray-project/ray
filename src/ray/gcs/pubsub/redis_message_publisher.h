#ifndef RAY_GCS_PUBSUB_REDIS_MESSAGE_PUBLISHER_H
#define RAY_GCS_PUBSUB_REDIS_MESSAGE_PUBLISHER_H

#include "ray/gcs/pubsub/message_publisher.h"
#include "ray/gcs/redis_client.h"

namespace ray {

namespace gcs {

class RedisMessagePublisher : public MessagePublisher {
 public:
  RedisMessagePublisher(std::shared_ptr<RedisClient> redis_client);

  Status Connect(std::shared_ptr<IOServicePool> io_service_pool) override;

  void Disconnect() override;

  Status PublishMessage(const std::string &channel, const std::string &message,
                        const StatusCallback &callback) override;

  template <typename Message>
  Status PublishMessage(const std::string &channel, const Message &message,
                        const StatusCallback &callback) override;

 private:
  std::shared_ptr<RedisClient> redis_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_PUBSUB_REDIS_MESSAGE_PUBLISHER_H
