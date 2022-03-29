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

#include "ray/gcs/pubsub/gcs_pub_sub.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"

namespace ray {

class GcsPubSubTest : public ::testing::Test {
 public:
  GcsPubSubTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~GcsPubSubTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    gcs::RedisClientOptions redis_client_options(
        "127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", /*enable_sharding_conn=*/false);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    RAY_CHECK_OK(client_->Connect(io_service_));
    pub_sub_ = std::make_shared<gcs::GcsPubSub>(client_);
  }

  virtual void TearDown() override {
    client_->Disconnect();
    io_service_.stop();
    thread_io_service_->join();
    thread_io_service_.reset();
    pub_sub_.reset();

    // Note: If we immediately reset client_ after io_service_ stop, because client_ still
    // has thread executing logic, such as unsubscribe's callback, the problem of heap
    // used after free will occur.
    client_.reset();
  }

  void Subscribe(const std::string &channel,
                 const std::string &id,
                 std::vector<std::string> &result) {
    std::promise<bool> promise;
    auto done = [&promise](const Status &status) { promise.set_value(status.ok()); };
    auto subscribe = [this, &result](const std::string &id, const std::string &data) {
      absl::MutexLock lock(&vector_mutex_);
      result.push_back(data);
    };
    RAY_CHECK_OK((pub_sub_->Subscribe(channel, id, subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  void SubscribeAll(const std::string &channel,
                    std::vector<std::pair<std::string, std::string>> &result) {
    std::promise<bool> promise;
    auto done = [&promise](const Status &status) { promise.set_value(status.ok()); };
    auto subscribe = [this, &result](const std::string &id, const std::string &data) {
      absl::MutexLock lock(&vector_mutex_);
      result.push_back(std::make_pair(id, data));
    };
    RAY_CHECK_OK(pub_sub_->SubscribeAll(channel, subscribe, done));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  void Unsubscribe(const std::string &channel, const std::string &id) {
    RAY_CHECK_OK(pub_sub_->Unsubscribe(channel, id));
  }

  bool Publish(const std::string &channel,
               const std::string &id,
               const std::string &data) {
    std::promise<bool> promise;
    auto done = [&promise](const Status &status) { promise.set_value(status.ok()); };
    RAY_CHECK_OK((pub_sub_->Publish(channel, id, data, done)));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename Data>
  void WaitPendingDone(const std::vector<Data> &data, int expected_count) {
    auto condition = [this, &data, expected_count]() {
      absl::MutexLock lock(&vector_mutex_);
      RAY_CHECK((int)data.size() <= expected_count)
          << "Expected " << expected_count << " data " << data.size();
      return (int)data.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  std::shared_ptr<gcs::RedisClient> client_;
  const std::chrono::milliseconds timeout_ms_{60000};
  std::shared_ptr<gcs::GcsPubSub> pub_sub_;
  absl::Mutex vector_mutex_;

 private:
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsPubSubTest, TestPubSubApi) {
  std::string channel("channel");
  std::string id("id");
  std::string data("data");
  std::vector<std::pair<std::string, std::string>> all_result;
  SubscribeAll(channel, all_result);
  std::vector<std::string> result;
  Subscribe(channel, id, result);
  Publish(channel, id, data);

  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  Unsubscribe(channel, id);
  Publish(channel, id, data);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(result.size(), 1);

  Subscribe(channel, id, result);
  Publish(channel, id, data);
  WaitPendingDone(result, 2);
  WaitPendingDone(all_result, 3);
}

TEST_F(GcsPubSubTest, TestManyPubsub) {
  std::string channel("channel");
  std::string id("id");
  std::string data("data");
  std::vector<std::pair<std::string, std::string>> all_result;
  SubscribeAll(channel, all_result);
  // Test many concurrent subscribes and unsubscribes.
  for (int i = 0; i < 1000; i++) {
    auto subscribe = [](const std::string &id, const std::string &data) {};
    RAY_CHECK_OK((pub_sub_->Subscribe(channel, id, subscribe, nullptr)));
    RAY_CHECK_OK((pub_sub_->Unsubscribe(channel, id)));
  }
  for (int i = 0; i < 1000; i++) {
    std::vector<std::string> result;
    // Use the synchronous subscribe to make sure our SUBSCRIBE message reaches
    // Redis before the PUBLISH.
    Subscribe(channel, id, result);
    Publish(channel, id, data);

    WaitPendingDone(result, 1);
    WaitPendingDone(all_result, i + 1);
    RAY_CHECK_OK((pub_sub_->Unsubscribe(channel, id)));
  }
}

TEST_F(GcsPubSubTest, TestMultithreading) {
  std::string channel("channel");
  auto sub_message_count = std::make_shared<std::atomic<int>>(0);
  auto sub_finished_count = std::make_shared<std::atomic<int>>(0);
  int size = 5;
  std::vector<std::unique_ptr<std::thread>> threads;
  threads.resize(size);
  for (int index = 0; index < size; ++index) {
    std::stringstream ss;
    ss << index;
    auto id = ss.str();
    threads[index].reset(
        new std::thread([this, sub_message_count, sub_finished_count, id, channel] {
          auto subscribe = [sub_message_count](const std::string &id,
                                               const std::string &data) {
            ++(*sub_message_count);
          };
          auto on_done = [sub_finished_count](const Status &status) {
            RAY_CHECK_OK(status);
            ++(*sub_finished_count);
          };
          RAY_CHECK_OK(pub_sub_->Subscribe(channel, id, subscribe, on_done));
        }));
  }
  auto sub_finished_condition = [sub_finished_count, size]() {
    return sub_finished_count->load() == size;
  };
  EXPECT_TRUE(WaitForCondition(sub_finished_condition, timeout_ms_.count()));
  for (auto &thread : threads) {
    thread->join();
    thread.reset();
  }

  std::string data("data");
  for (int index = 0; index < size; ++index) {
    std::stringstream ss;
    ss << index;
    auto id = ss.str();
    threads[index].reset(new std::thread([this, channel, id, data] {
      RAY_CHECK_OK(pub_sub_->Publish(channel, id, data, nullptr));
    }));
  }

  auto sub_message_condition = [sub_message_count, size]() {
    return sub_message_count->load() == size;
  };
  EXPECT_TRUE(WaitForCondition(sub_message_condition, timeout_ms_.count()));
  for (auto &thread : threads) {
    thread->join();
    thread.reset();
  }
}

TEST_F(GcsPubSubTest, TestPubSubWithTableData) {
  std::string channel("channel");
  std::string data("data");
  std::vector<std::string> result;
  int size = 1000;

  for (int index = 0; index < size; ++index) {
    ObjectID object_id = ObjectID::FromRandom();
    std::promise<bool> promise;
    auto done = [&promise](const Status &status) { promise.set_value(status.ok()); };
    auto subscribe = [this, channel, &result](const std::string &id,
                                              const std::string &data) {
      RAY_CHECK_OK(pub_sub_->Unsubscribe(channel, id));
      absl::MutexLock lock(&vector_mutex_);
      result.push_back(data);
    };
    RAY_CHECK_OK((pub_sub_->Subscribe(channel, object_id.Hex(), subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
    RAY_CHECK_OK((pub_sub_->Publish(channel, object_id.Hex(), data, nullptr)));
  }

  WaitPendingDone(result, size);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
