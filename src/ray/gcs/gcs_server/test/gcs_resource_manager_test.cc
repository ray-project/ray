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

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest() {
    std::promise<bool> promise;
    thread_io_service_.reset(new std::thread([this, &promise] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      promise.set_value(true);
      io_service_.run();
    }));
    promise.get_future().get();

    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
  }

  virtual ~GcsResourceManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
  }

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GcsResourceManagerTest, TestBasic) {}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
