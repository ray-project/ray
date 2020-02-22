#include "ray/gcs/test/accessor_test_base.h"

#include <boost/asio/io_service.hpp>

namespace ray {

namespace gcs {

AccessorTestRawBase::AccessorTestRawBase() : io_service_(new boost::asio::io_service()) {}

AccessorTestRawBase::~AccessorTestRawBase() {}

void AccessorTestRawBase::SetUp() {
  GenTestData();

  GcsClientOptions options = GcsClientOptions("127.0.0.1", REDIS_SERVER_PORT, "", true);
  gcs_client_.reset(new RedisGcsClient(options));
  RAY_CHECK_OK(gcs_client_->Connect(*io_service_));

  work_thread_.reset(new std::thread([this] {
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(*io_service_));
    io_service_->run();
  }));
}

void AccessorTestRawBase::TearDown() {
  gcs_client_->Disconnect();

  io_service_->stop();
  work_thread_->join();
  work_thread_.reset();

  gcs_client_.reset();

  ClearTestData();
}

void AccessorTestRawBase::WaitPendingDone(std::chrono::milliseconds timeout) {
  WaitPendingDone(pending_count_, timeout);
}

void AccessorTestRawBase::WaitPendingDone(std::atomic<int> &pending_count,
                                          std::chrono::milliseconds timeout) {
  auto condition = [&pending_count]() { return pending_count == 0; };
  EXPECT_TRUE(WaitForCondition(condition, timeout.count()));
}

}  // namespace gcs

}  // namespace ray
