#include "ray/gcs/gcs_server/gcs_publisher_manager.h"

namespace ray {
namespace gcs {

GcsPublisherManager::GcsPublisherManager(instrumented_io_context &service)
    : publisher_service_(service),
      periodical_runner_(service),
      grpc_publisher_(
          /*periodical_runner=*/&periodical_runner_,
          /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
          /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
          /*publish_batch_size_=*/RayConfig::instance().publish_batch_size()) {}

GcsPublisherManager::~GcsPublisherManager() { Stop(); }

void GcsPublisherManager::Start() {
  publisher_thread_.reset(new std::thread([this] {
    SetThreadName("publisher");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(publisher_service_);
    publisher_service_.run();
  }));
}

void GcsPublisherManager::Stop() {
  if (publisher_thread_ != nullptr) {
    // TODO (Alex): There's technically a race condition here if we start and stop the
    // thread in rapid succession.
    publisher_service_.stop();
    if (publisher_thread_->joinable()) {
      publisher_thread_->join();
    }
  }
}

void GcsPublisherManager::Publish(const rpc::ChannelType channel_type,
                                  const rpc::PubMessage pub_message,
                                  const std::string key_id_binary) {
  publisher_service_.post([this, channel_type, pub_message, key_id_binary]() {
    grpc_publisher_.Publish(channel_type, pub_message, key_id_binary);
  });
}

void GcsPublisherManager::HandlePubsubLongPolling(
    const rpc::PubsubLongPollingRequest &request, rpc::PubsubLongPollingReply *reply,
    rpc::SendReplyCallback callback) {
  const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());
  auto wrapped_callback = [callback = std::move(callback), reply](
                              Status status, std::function<void()> success,
                              std::function<void(void)> failure) {
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };

  grpc_publisher_.ConnectToSubscriber(subscriber_id, reply, wrapped_callback);
}

void GcsPublisherManager::HandlePubsubCommandBatch(
    const rpc::PubsubCommandBatchRequest &request, rpc::PubsubCommandBatchReply *reply,
    rpc::SendReplyCallback callback) {
  const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());

  Status status = Status::OK();

  for (const auto &command : request.commands()) {
    if (command.has_unsubscribe_message()) {
      grpc_publisher_.UnregisterSubscription(command.channel_type(), subscriber_id,
                                             command.key_id());
    } else if (command.has_subscribe_message()) {
      grpc_publisher_.RegisterSubscription(command.channel_type(), subscriber_id,
                                           command.key_id());
    } else {
      RAY_LOG(ERROR) << "Invalid command has received, "
                     << static_cast<int>(command.command_message_one_of_case())
                     << ". If you see this message, please "
                        "report to Ray "
                        "Github.";
      std::stringstream stream;
      stream << "Invalid pubsub command received: " << request.DebugString();
      status = Status::Invalid(stream.str());
    }
  }

  GCS_RPC_SEND_REPLY(callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
