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
#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class GcsPubSubTest : public RedisServiceManagerForTest {
 protected:
  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    gcs::RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "",
                                                 true);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    RAY_CHECK_OK(client_->Connect(io_service_));
    pub_sub_ = std::make_shared<gcs::GcsPubSub>(client_);
  }

  virtual void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  template <typename ID, typename Data>
  void Subscribe(const gcs::TablePubsub &channel, const ID &id,
                 std::vector<Data> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const ID &id, const Data &data) {
      result.push_back(data);
    };
    RAY_CHECK_OK((pub_sub_->Subscribe<ID, Data>(channel, id, subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename ID, typename Data>
  void SubscribeAll(const gcs::TablePubsub &channel,
                    std::vector<std::pair<ID, Data>> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const ID &id, const Data &data) {
      result.push_back(std::make_pair(id, data));
    };
    RAY_CHECK_OK((pub_sub_->SubscribeAll<ID, Data>(channel, subscribe, done)));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename ID>
  bool Unsubscribe(const gcs::TablePubsub &channel, const ID &id) {
    return pub_sub_->Unsubscribe<ID>(channel, id).ok();
  }

  template <typename ID, typename Data>
  bool Publish(const gcs::TablePubsub &channel, const ID &id, const Data &data) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    RAY_CHECK_OK((pub_sub_->Publish<ID, Data>(channel, id, data, done)));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  template <typename Data>
  void WaitPendingDone(const std::vector<Data> &data, int expected_count) {
    auto condition = [&data, expected_count]() {
      return (int)data.size() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  std::shared_ptr<gcs::RedisClient> client_;
  JobID job_id_ = JobID::FromInt(1);
  const std::chrono::milliseconds timeout_ms_{60000};

  std::shared_ptr<gcs::GcsPubSub> pub_sub_;

 private:
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsPubSubTest, TestJobTablePubSubApi) {
  const auto &channel = gcs::TablePubsub::JOB_PUBSUB;
  rpc::JobTableData job_table_data;
  job_table_data.set_job_id(job_id_.Binary());

  std::vector<std::pair<JobID, rpc::JobTableData>> all_result;

  SubscribeAll<JobID, rpc::JobTableData>(channel, all_result);
  std::vector<rpc::JobTableData> result;
  Subscribe<JobID, rpc::JobTableData>(channel, job_id_, result);
  Publish<JobID, rpc::JobTableData>(channel, job_id_, job_table_data);

  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  Unsubscribe<JobID>(channel, job_id_);
  Publish<JobID, rpc::JobTableData>(channel, job_id_, job_table_data);
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);

  Subscribe<JobID, rpc::JobTableData>(channel, job_id_, result);
  Publish<JobID, rpc::JobTableData>(channel, job_id_, job_table_data);
  WaitPendingDone(result, 2);
  WaitPendingDone(all_result, 3);
}

TEST_F(GcsPubSubTest, TestMultithreading) {
  const auto &channel = gcs::TablePubsub::ACTOR_PUBSUB;
  auto count = std::make_shared<std::atomic<int>>(0);
  int size = 5;
  for (int index = 0; index < size; ++index) {
    new std::thread([this, count, index, channel] {
      auto subscribe = [count](const ActorID &id, const rpc::ActorTableData &data) {
        ++(*count);
      };
      auto id = ActorID::Of(JobID::FromInt(index), TaskID::Nil(), 0);
      RAY_CHECK_OK((pub_sub_->Subscribe<ActorID, rpc::ActorTableData>(
          channel, id, subscribe, nullptr)));
    });
  }

  for (int index = 0; index < size; ++index) {
    new std::thread([this, count, index, channel] {
      auto id = ActorID::Of(JobID::FromInt(index), TaskID::Nil(), 0);
      rpc::ActorTableData data;
      RAY_CHECK_OK(
          (pub_sub_->Publish<ActorID, rpc::ActorTableData>(channel, id, data, nullptr)));
    });
  }

  auto condition = [count, size]() { return count->load() == size; };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
