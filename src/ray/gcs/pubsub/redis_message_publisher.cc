#include "ray/gcs/pubsub/redis_message_publisher.h"
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

RedisMessagePublisher::RedisMessagePublisher(
    const RedisClientOptions &options, std::shared_ptr<IOServicePool> io_service_pool)
    : redis_client_(new RedisClient(options)),
      io_service_pool_(std::move(io_service_pool)) {}

Status RedisMessagePublisher::Init() {
  auto io_services = io_service_pool_->GetAll();
  Status status = redis_client_->Connect(io_services);
  RAY_LOG(INFO) << "RedisMessagePublisher::Init finished with status "
                << status.ToString();
  return status;
}

void RedisMessagePublisher::Shutdown() {
  redis_client_->Disconnect();
  RAY_LOG(INFO) << "RedisMessagePublisher::Shutdown.";
}

Status RedisMessagePublisher::PublishMessage(const std::string &channel,
                                             const std::string &message,
                                             const StatusCallback &callback) {
  std::vector<std::string> args = {"PUBLISH", channel, message};

  RedisCallback pub_callback = nullptr;
  if (callback) {
    pub_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      callback(Status::OK());
    };
  }
  // Select shard context by channel.
  auto shard_context = redis_client_->GetShardContext(channel);
  return shard_context->RunArgvAsync(args, pub_callback);
}

}  // namespace gcs

}  // namespace ray
