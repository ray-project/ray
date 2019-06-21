#include "ray/gcs/gcs_client.h"
#include "ray/gcs/client.h"
#include "ray/gcs/actor_state_accessor.h"
#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/task_state_accessor.h"

namespace ray {

namespace gcs {

GcsClient::GcsClient(ClientOption option, ClientInfo info,
                     boost::asio::io_service &io_service)
    : option_(std::move(option_)),
      info_(std::move(info)),
      io_service_(&io_service) {
}

GcsClient::GcsClient(ClientOption option, ClientInfo info)
    : option_(std::move(option_)),
      info_(std::move(info)) {
}

GcsClient::~GcsClient() {
  delete node_state_accessor_;
  node_state_accessor_ = nullptr;

  delete actor_state_accessor_;
  actor_state_accessor_ = nullptr;

  delete task_state_accessor_;
  task_state_accessor_ = nullptr;

  delete client_impl_;
  client_impl_ = nullptr;

  if (!thread_pool_.empty()) {
    delete io_service_;
    io_service_ = nullptr;
  }
}

Status GcsClient::Connect() {
  if (option_.server_list_.empty()) {
    return Status::Invalid("gcs service address is empty!");
  }

  const std::string &address = option_.server_list_[0].first;
  int port = option_.server_list_[0].second;

  client_impl_ = new AsyncGcsClient(address, port, info_.id_, option_.command_type_,
                                    option_.test_mode_, option_.password_);

  node_state_accessor_ = new NodeStateAccessor(client_impl_);
  actor_state_accessor_ = new ActorStateAccessor(client_impl_);
  task_state_accessor_ = new TaskStateAccessor(client_impl_);

  if (io_service_ == nullptr) {
    // init thread pool
    io_service_ = new boost::asio::io_service;
    thread_pool_.emplace_back([this] {
      boost::asio::io_service::work *worker
        = new boost::asio::io_service::work(*io_service_);
      io_service_->run();
      delete worker;
    });
  }

  return client_impl_->Attach(*io_service_);
}

Status GcsClient::Disconnect() {
  if (!thread_pool_.empty()) {
    io_service_->stop();

    for (auto &thread : thread_pool_) {
      thread.join();
    }
  }
  // TODO(micafan) AsyncGcsClient disconnect
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
