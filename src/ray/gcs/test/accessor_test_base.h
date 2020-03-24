// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_GCS_ACCESSOR_TEST_BASE_H
#define RAY_GCS_ACCESSOR_TEST_BASE_H

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

template <typename ID, typename Data>
class AccessorTestBase : public RedisServiceManagerForTest {
 public:
  AccessorTestBase() {}

  virtual ~AccessorTestBase() {}

  virtual void SetUp() {
    GenTestData();

    GcsClientOptions options = GcsClientOptions("127.0.0.1", REDIS_SERVER_PORT, "", true);
    gcs_client_.reset(new RedisGcsClient(options));
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
