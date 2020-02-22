#ifndef RAY_GCS_ACCESSOR_TEST_BASE_H
#define RAY_GCS_ACCESSOR_TEST_BASE_H

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/test_util.h"

namespace ray {

namespace gcs {

class AccessorTestRawBase : public RedisServiceManagerForTest {
 public:
  AccessorTestRawBase();

  virtual ~AccessorTestRawBase();

  virtual void SetUp();

  virtual void TearDown();

 protected:
  virtual void GenTestData() = 0;

  virtual void ClearTestData() = 0;

  void WaitPendingDone(std::chrono::milliseconds timeout);

  void WaitPendingDone(std::atomic<int> &pending_count,
                       std::chrono::milliseconds timeout);

 protected:
  std::unique_ptr<RedisGcsClient> gcs_client_;

  std::unique_ptr<boost::asio::io_service> io_service_;
  std::unique_ptr<std::thread> work_thread_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{10000};
};

template <typename ID, typename Data>
class AccessorTestBase : public AccessorTestRawBase {
 protected:
  void ClearTestData() { id_to_data_.clear(); }

  std::unordered_map<ID, std::shared_ptr<Data>> id_to_data_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACCESSOR_TEST_BASE_H
