#ifndef RAY_GCS_ACCESSOR_TEST_BASE_H
#define RAY_GCS_ACCESSOR_TEST_BASE_H

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/test_util.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data>
class AccessorTestBase : public ::testing::Test {
 public:
  AccessorTestBase() : options_("127.0.0.1", 6379, "", true) {}

  virtual ~AccessorTestBase() {}

  virtual void SetUp() {
    GenTestData();

    gcs_client_.reset(new RedisGcsClient(options_));
    RAY_CHECK_OK(gcs_client_->Connect(io_service_));

    work_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  virtual void TearDown() {
    gcs_client_->Disconnect();

    io_service_.stop();
    work_thread_->join();
    work_thread_.reset();

    gcs_client_.reset();

    ClearTestData();
  }

 protected:
  virtual void GenTestData() = 0;

  void ClearTestData() { id_to_data_.clear(); }

  void WaitPendingDone(std::chrono::milliseconds timeout) {
    WaitPendingDone(pending_count_, timeout);
  }

  void WaitPendingDone(std::atomic<int> &pending_count,
                       std::chrono::milliseconds timeout) {
    auto condition = [&pending_count]() { return pending_count == 0; };
    EXPECT_TRUE(WaitForCondition(condition, timeout.count()));
  }

 protected:
  GcsClientOptions options_;
  std::unique_ptr<RedisGcsClient> gcs_client_;

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread_;

  std::unordered_map<ID, std::shared_ptr<Data>> id_to_data_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{10000};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACCESSOR_TEST_BASE_H
