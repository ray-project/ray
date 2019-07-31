#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/redis_gcs_client.h"

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

    work_thread.reset(new std::thread([this] {
      std::auto_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  virtual void TearDown() {
    gcs_client_->Disconnect();

    io_service_.stop();
    work_thread->join();
    work_thread.reset();

    gcs_client_.reset();

    ClearTestData();
  }

 protected:
  virtual void GenTestData() = 0;

  void ClearTestData() { id_to_datas_.clear(); }

  void WaitPendingDone(std::chrono::milliseconds timeout) {
    WaitPendingDone(pending_count_, timeout);
  }

  void WaitPendingDone(std::atomic<int> &pending_count,
                       std::chrono::milliseconds timeout) {
    while (pending_count != 0 && timeout.count() > 0) {
      std::chrono::milliseconds interval(10);
      std::this_thread::sleep_for(interval);
      timeout -= interval;
    }
    EXPECT_EQ(pending_count, 0);
  }

 protected:
  GcsClientOptions options_;
  std::unique_ptr<RedisGcsClient> gcs_client_;

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread;

  std::unordered_map<ID, std::shared_ptr<Data>> id_to_datas_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{10000};
};

}  // namespace gcs

}  // namespace ray
