#include "ray/gcs/gcs_client_impl.h"

namespace ray {

namespace gcs {

GcsClientImpl::GcsClientImpl(ClientOption option, ClientInfo info,
                             boost::asio::io_service *io_service)
    : option_(std::move(option)), info_(std::move(info)), io_service_(io_service) {}

GcsClientImpl::~GcsClientImpl() {
  if (!thread_pool_.empty()) {
    RAY_CHECK(io_service_->stopped());
    delete io_service_;
    io_service_ = nullptr;
  }
}

Status GcsClientImpl::Connect() {
  if (option_.server_list_.empty()) {
    RAY_LOG(INFO) << "connect failed, gcs service address is empty.";
    return Status::Invalid("gcs service address is empty!");
  }

  const std::string &address = option_.server_list_[0].first;
  int port = option_.server_list_[0].second;

  async_gcs_client_.reset(new AsyncGcsClient(address, port, info_.id_,
                                             option_.command_type_, option_.test_mode_,
                                             option_.password_));

  if (io_service_ == nullptr) {
    // init thread pool
    io_service_ = new boost::asio::io_service;
    thread_pool_.emplace_back([this] {
      boost::asio::io_service::work *worker =
          new boost::asio::io_service::work(*io_service_);
      io_service_->run();
      delete worker;
    });
  }

  Status status = async_gcs_client_->Attach(*io_service_);
  RAY_LOG(INFO) << "Connect status=" << status;
  return status;
}

void GcsClientImpl::Disconnect() {
  if (!thread_pool_.empty()) {
    io_service_->stop();

    for (auto &thread : thread_pool_) {
      thread.join();
    }
  }

  // TODO(micafan) AsyncGcsClient disconnect
}

}  // namespace gcs

}  // namespace ray
