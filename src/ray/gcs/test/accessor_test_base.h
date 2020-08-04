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

#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class MockActorInfoAccessor : public ActorInfoAccessor {
 public:
  MockActorInfoAccessor(gcs::RedisGcsClient *client) : client_impl_(client) {}

  Status GetAll(std::vector<ActorTableData> *actor_table_data_list) override {
    return Status::OK();
  }

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetByName(
      const std::string &name,
      const OptionalItemCallback<rpc::ActorTableData> &callback) override {
    return Status::OK();
  }

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const StatusCallback &callback) override {
    if (RayConfig::instance().gcs_actor_service_enabled()) {
      auto on_register_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                                         const ActorTableData &data) {
        if (callback != nullptr) {
          callback(Status::OK());
        }
      };
      ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
      return client_impl_->actor_table().Add(JobID::Nil(), actor_id, data_ptr,
                                             on_register_done);
    } else {
      auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                                   const ActorTableData &data) {
        if (callback != nullptr) {
          callback(Status::OK());
        }
      };

      auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id,
                                   const ActorTableData &data) {
        if (callback != nullptr) {
          callback(Status::Invalid("Adding actor failed."));
        }
      };

      ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
      return client_impl_->log_based_actor_table().AppendAt(
          actor_id.JobId(), actor_id, data_ptr, on_success, on_failure,
          /*log_length*/ 0);
    }
  }

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                     const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done) override {
    return Status::OK();
  }

  Status AsyncUnsubscribe(const ActorID &actor_id) override { return Status::OK(); }

  Status AsyncAddCheckpoint(const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback) override {
    return Status::OK();
  }

  Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointData> &callback) override {
    return Status::OK();
  }

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) override {
    return Status::OK();
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  RedisGcsClient *client_impl_{nullptr};
};

class MockGcs : public gcs::RedisGcsClient {
 public:
  MockGcs(GcsClientOptions &option) : gcs::RedisGcsClient(option){};

  void Init(MockActorInfoAccessor *mock_actor_accessor) {
    actor_accessor_.reset(mock_actor_accessor);
  }
};

template <typename ID, typename Data>
class AccessorTestBase : public ::testing::Test {
 public:
  AccessorTestBase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~AccessorTestBase() { TestSetupUtil::ShutDownRedisServers(); }

  virtual void SetUp() {
    GenTestData();

    GcsClientOptions options =
        GcsClientOptions("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", true);
    gcs_client_.reset(new MockGcs(options));
    MockActorInfoAccessor *mock_actor_accessor =
        new MockActorInfoAccessor(gcs_client_.get());
    gcs_client_->Init(mock_actor_accessor);
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
  std::unique_ptr<MockGcs> gcs_client_;

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread_;

  std::unordered_map<ID, std::shared_ptr<Data>> id_to_data_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{10000};
};

}  // namespace gcs

}  // namespace ray
