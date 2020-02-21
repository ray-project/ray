#include "ray/gcs/test/accessor_test_base.h"

#include <boost/asio/io_service.hpp>

namespace ray {

namespace gcs {

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

}  // namespace gcs

}  // namespace ray
