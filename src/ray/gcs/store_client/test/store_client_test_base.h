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
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/id.h"
#include "ray/common/test_util.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class StoreClientTestBase : public ::testing::Test {
 public:
  StoreClientTestBase() = default;

  virtual ~StoreClientTestBase() {}

  void SetUp() override {
    io_service_pool_ = std::make_shared<IOServicePool>(io_service_num_);
    io_service_pool_->Run();
    InitStoreClient();
    GenTestData();
  }

  void TearDown() override {
    DisconnectStoreClient();

    io_service_pool_->Stop();

    key_to_value_.clear();
  }

  virtual void InitStoreClient() = 0;

  virtual void DisconnectStoreClient() = 0;

 protected:
  void Put() {
    auto put_calllback = [this](auto) { --pending_count_; };
    for (const auto &[key, value] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncPut(
          table_name_, key.Binary(), value.SerializeAsString(), true, put_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncPut(
          table_name_, key.Binary(), value.SerializeAsString(), true, nullptr));
    }
    WaitPendingDone();
  }

  void Delete() {
    auto delete_calllback = [this](auto) { --pending_count_; };
    for (const auto &[key, _] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(
          store_client_->AsyncDelete(table_name_, key.Binary(), delete_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncDelete(table_name_, key.Binary(), nullptr));
    }
    WaitPendingDone();
  }

  void Get() {
    auto get_callback = [this](const Status &status,
                               const boost::optional<std::string> &result) {
      RAY_CHECK_OK(status);
      RAY_CHECK(result);
      rpc::ActorTableData data;
      RAY_CHECK(data.ParseFromString(*result));
      ActorID actor_id = ActorID::FromBinary(data.actor_id());
      auto it = key_to_value_.find(actor_id);
      RAY_CHECK(it != key_to_value_.end());
      --pending_count_;
    };
    for (const auto &[key, _] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncGet(table_name_, key.Binary(), get_callback));
    }
    WaitPendingDone();
  }

  void GetEmpty() {
    for (const auto &[k, _] : key_to_value_) {
      auto key = k.Binary();
      auto get_callback = [this, key](const Status &status,
                                      const boost::optional<std::string> &result) {
        RAY_CHECK_OK(status);
        RAY_CHECK(!result);
        --pending_count_;
      };

      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncGet(table_name_, key, get_callback));
    }
    WaitPendingDone();
  }

  void GetAll() {
    auto get_all_callback =
        [this](const absl::flat_hash_map<std::string, std::string> &result) {
          static std::unordered_set<ActorID> received_keys;
          for (const auto &item : result) {
            const ActorID &actor_id = ActorID::FromBinary(item.first);
            auto it = received_keys.find(actor_id);
            RAY_CHECK(it == received_keys.end());
            received_keys.emplace(actor_id);

            auto map_it = key_to_value_.find(actor_id);
            RAY_CHECK(map_it != key_to_value_.end());
          }
          RAY_CHECK(received_keys.size() == key_to_value_.size());
          pending_count_ -= result.size();
        };

    pending_count_ += key_to_value_.size();
    RAY_CHECK_OK(store_client_->AsyncGetAll(table_name_, get_all_callback));
    WaitPendingDone();
  }

  void GetKeys() {
    for (int i = 0; i < 100; i++) {
      auto key = keys_.at(std::rand() % keys_.size()).Binary();
      auto prefix = key.substr(0, std::rand() % key.size());
      RAY_LOG(INFO) << "key is: " << key << ", prefix is: " << prefix;
      std::unordered_set<std::string> result_set;
      for (const auto &item1 : key_to_value_) {
        if (item1.first.Binary().find(prefix) == 0) {
          result_set.insert(item1.first.Binary());
        }
      }
      ASSERT_FALSE(result_set.empty());

      auto get_keys_callback = [this,
                                &result_set](const std::vector<std::string> result) {
        std::unordered_set<std::string> received_keys(result.begin(), result.end());
        ASSERT_EQ(received_keys, result_set);
        pending_count_ -= result.size();
      };

      pending_count_ += result_set.size();

      RAY_CHECK_OK(store_client_->AsyncGetKeys(table_name_, prefix, get_keys_callback));
      WaitPendingDone();
    }
  }

  void Exists(bool exists) {
    auto exists_callback = [this, exists](bool result) {
      ASSERT_TRUE(result == exists);
      pending_count_--;
    };

    pending_count_ += key_to_value_.size();
    for (const auto &item : key_to_value_) {
      RAY_CHECK_OK(
          store_client_->AsyncExists(table_name_, item.first.Binary(), exists_callback));
    }
    WaitPendingDone();
  }

  void BatchDelete() {
    auto delete_calllback = [this](auto) { --pending_count_; };
    ++pending_count_;
    std::vector<std::string> keys;
    for (auto &[key, _] : key_to_value_) {
      keys.push_back(key.Binary());
    }
    RAY_CHECK_OK(store_client_->AsyncBatchDelete(table_name_, keys, delete_calllback));
    // Make sure no-op callback is handled well
    RAY_CHECK_OK(store_client_->AsyncBatchDelete(table_name_, keys, nullptr));
    WaitPendingDone();
  }

  void TestAsyncPutAndAsyncGet() {
    // AsyncPut without index.
    Put();

    // AsyncGet
    Get();

    // AsyncDelete
    Delete();

    GetEmpty();
  }

  void TestAsyncGetAllAndBatchDelete() {
    Exists(false);
    // AsyncPut
    Put();

    // AsyncGetAll
    GetAll();

    GetKeys();
    Exists(true);

    // AsyncBatchDelete
    BatchDelete();

    Exists(false);
    // AsyncGet
    GetEmpty();
  }

  void GenTestData() {
    for (size_t i = 0; i < key_count_; i++) {
      rpc::ActorTableData actor;
      actor.set_max_restarts(1);
      actor.set_num_restarts(0);
      JobID job_id = JobID::FromInt(i % index_count_);
      actor.set_job_id(job_id.Binary());
      actor.set_state(rpc::ActorTableData::ALIVE);
      ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), /*parent_task_counter=*/i);
      actor.set_actor_id(actor_id.Binary());

      key_to_value_[actor_id] = actor;
      keys_.push_back(actor_id);
    }
  }

  void WaitPendingDone() { WaitPendingDone(pending_count_); }

  void WaitPendingDone(std::atomic<int> &pending_count) {
    auto condition = [&pending_count]() { return pending_count == 0; };
    EXPECT_TRUE(WaitForCondition(condition, wait_pending_timeout_.count()));
  }

 protected:
  size_t io_service_num_{2};
  std::shared_ptr<IOServicePool> io_service_pool_;

  std::shared_ptr<StoreClient> store_client_;

  std::string table_name_{"test_table"};
  size_t key_count_{5000};
  size_t index_count_{100};
  absl::flat_hash_map<ActorID, rpc::ActorTableData> key_to_value_;
  std::vector<ActorID> keys_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{5000};
};

}  // namespace gcs

}  // namespace ray
