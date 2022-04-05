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
    key_to_index_.clear();
    index_to_keys_.clear();
  }

  virtual void InitStoreClient() = 0;

  virtual void DisconnectStoreClient() = 0;

 protected:
  void Put() {
    auto put_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    for (const auto &[key, value] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncPut(
          table_name_, key.Binary(), value.SerializeAsString(), put_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncPut(
          table_name_, key.Binary(), value.SerializeAsString(), nullptr));
    }
    WaitPendingDone();
  }

  void Delete() {
    auto delete_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
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

  void PutWithIndex() {
    auto put_calllback = [this](const Status &status) { --pending_count_; };
    for (const auto &[key, value] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncPutWithIndex(table_name_,
                                                    key.Binary(),
                                                    key_to_index_[key].Hex(),
                                                    value.SerializeAsString(),
                                                    put_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncPutWithIndex(table_name_,
                                                    key.Binary(),
                                                    key_to_index_[key].Hex(),
                                                    value.SerializeAsString(),
                                                    nullptr));
    }
    WaitPendingDone();
  }

  void GetByIndex() {
    auto get_calllback =
        [this](const absl::flat_hash_map<std::string, std::string> &result) {
          if (!result.empty()) {
            auto key = ActorID::FromBinary(result.begin()->first);
            auto it = key_to_index_.find(key);
            RAY_CHECK(it != key_to_index_.end());
            RAY_CHECK(index_to_keys_[it->second].size() == result.size());
          }
          pending_count_ -= result.size();
        };
    auto iter = index_to_keys_.begin();
    pending_count_ += iter->second.size();
    RAY_CHECK_OK(
        store_client_->AsyncGetByIndex(table_name_, iter->first.Hex(), get_calllback));
    WaitPendingDone();
  }

  void DeleteByIndex() {
    auto delete_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    for (const auto &[key, _] : index_to_keys_) {
      ++pending_count_;
      RAY_CHECK_OK(
          store_client_->AsyncDeleteByIndex(table_name_, key.Hex(), delete_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncDeleteByIndex(table_name_, key.Hex(), nullptr));
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

  void BatchDelete() {
    auto delete_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
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

  void DeleteWithIndex() {
    auto delete_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    for (const auto &[key, _] : key_to_value_) {
      ++pending_count_;
      RAY_CHECK_OK(store_client_->AsyncDeleteWithIndex(
          table_name_, key.Binary(), key_to_index_[key].Hex(), delete_calllback));
      // Make sure no-op callback is handled well
      RAY_CHECK_OK(store_client_->AsyncDeleteWithIndex(
          table_name_, key.Binary(), key_to_index_[key].Hex(), nullptr));
    }
    WaitPendingDone();
  }

  void BatchDeleteWithIndex() {
    auto delete_calllback = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    std::vector<std::string> keys;
    std::vector<std::string> index_keys;
    for (auto &[key, _] : key_to_value_) {
      keys.push_back(key.Binary());
      index_keys.push_back(key_to_index_[key].Hex());
    }
    RAY_CHECK_OK(store_client_->AsyncBatchDeleteWithIndex(
        table_name_, keys, index_keys, delete_calllback));
    // Make sure no-op callback is handled well
    RAY_CHECK_OK(
        store_client_->AsyncBatchDeleteWithIndex(table_name_, keys, index_keys, nullptr));
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

  void TestAsyncPutAndDeleteWithIndex() {
    // AsyncPut with index
    PutWithIndex();

    // AsyncGet with index
    GetByIndex();

    // AsyncDelete by index
    DeleteByIndex();

    // AsyncGet
    GetEmpty();
  }

  void TestAsyncGetAllAndBatchDelete() {
    // AsyncPut
    Put();

    // AsyncGetAll
    GetAll();

    // AsyncBatchDelete
    BatchDelete();

    // AsyncGet
    GetEmpty();
  }

  void TestAsyncDeleteWithIndex() {
    // AsyncPut with index
    PutWithIndex();

    // AsyncGet with index
    GetByIndex();

    // AsyncDelete key-value and index-key
    DeleteWithIndex();

    // AsyncGet
    GetEmpty();
  }

  void TestAsyncBatchDeleteWithIndex() {
    // AsyncPut with index
    PutWithIndex();

    // AsyncGetAll
    GetByIndex();

    // AsyncBatchDeleteWithIndex
    BatchDeleteWithIndex();

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

      key_to_index_[actor_id] = job_id;

      auto it = index_to_keys_.find(job_id);
      if (it != index_to_keys_.end()) {
        it->second.emplace(actor_id);
      } else {
        std::unordered_set<ActorID> key_set;
        key_set.emplace(actor_id);
        index_to_keys_.emplace(job_id, std::move(key_set));
      }
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
  absl::flat_hash_map<ActorID, JobID> key_to_index_;
  absl::flat_hash_map<JobID, std::unordered_set<ActorID>> index_to_keys_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{5000};
};

}  // namespace gcs

}  // namespace ray
