#include "ray/gcs/gcs_client_impl.h"

namespace ray {

namespace gcs {

GcsClientImpl::GcsClientImpl(ClientOption option, ClientInfo info,
                             boost::asio::io_service *io_service)
    : option_(std::move(option)), info_(std::move(info)), io_service_(io_service) {}

GcsClientImpl::~GcsClientImpl() { RAY_CHECK(!is_connected_); }

Status GcsClientImpl::Connect() {
  if (is_connected_) {
    return Status::OK();
  }

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
    // Create event pool if necessary
    io_service_ = new boost::asio::io_service;
    thread_pool_.emplace_back([this] {
      std::auto_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*io_service_));
      io_service_->run();
    });
  }

  Status status = async_gcs_client_->Attach(*io_service_);
  is_connected_ = status.ok() ? true : false;
  RAY_LOG(INFO) << "GCS Client Connect status=" << status;
  return status;
}

void GcsClientImpl::Disconnect() {
  if (!is_connected_) {
    return;
  }

  if (!thread_pool_.empty()) {
    io_service_->stop();

    for (auto &thread : thread_pool_) {
      thread.join();
    }

    RAY_CHECK(io_service_->stopped());
    delete io_service_;
    io_service_ = nullptr;

    thread_pool_.clear();
  }
  is_connected_ = false;
  RAY_LOG(INFO) << "GCS Client Disconnect";
}

}  // namespace gcs

}  // namespace ray
