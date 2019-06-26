#include "ray/gcs/gcs_client.h"
#include "ray/gcs/gcs_client_impl.h"

namespace ray {

namespace gcs {

GcsClient::GcsClient(ClientOption option, ClientInfo info,
                     boost::asio::io_service &io_service)
    : client_impl_(new GcsClientImpl(std::move(option), std::move(info), &io_service)),
      node_accessor_(new NodeStateAccessor(*client_impl_)),
      actor_accessor_(new ActorStateAccessor(*client_impl_)),
      task_accessor_(new TaskStateAccessor(*client_impl_)) {}

GcsClient::GcsClient(ClientOption option, ClientInfo info)
    : client_impl_(new GcsClientImpl(std::move(option), std::move(info), nullptr)),
      node_accessor_(new NodeStateAccessor(*client_impl_)),
      actor_accessor_(new ActorStateAccessor(*client_impl_)),
      task_accessor_(new TaskStateAccessor(*client_impl_)) {}

Status GcsClient::Connect() { return client_impl_->Connect(); }

void GcsClient::Disconnect() { client_impl_->Disconnect(); }

}  // namespace gcs

}  // namespace ray
