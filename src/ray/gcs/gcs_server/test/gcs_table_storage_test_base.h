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

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

namespace gcs {

class GcsTableStorageTestBase : public ::testing::Test {
 public:
  GcsTableStorageTestBase() {
    io_service_pool_ = std::make_shared<IOServicePool>(io_service_num_);
    io_service_pool_->Run();
  }

  virtual ~GcsTableStorageTestBase() { io_service_pool_->Stop(); }

 protected:
  void TestGcsTableApi() {
    auto table = gcs_table_storage_->JobTable();
    JobID job1_id = JobID::FromInt(1);
    JobID job2_id = JobID::FromInt(2);
    auto job1_table_data = Mocker::GenJobTableData(job1_id);
    auto job2_table_data = Mocker::GenJobTableData(job2_id);

    // Put.
    Put(table, job1_id, *job1_table_data);
    Put(table, job2_id, *job2_table_data);

    // Get.
    std::vector<rpc::JobTableData> values;
    ASSERT_EQ(Get(table, job2_id, values), 1);
    ASSERT_EQ(Get(table, job2_id, values), 1);

    // Delete.
    Delete(table, job1_id);
    Delete(table, job2_id);
    ASSERT_EQ(Get(table, job1_id, values), 0);
    ASSERT_EQ(Get(table, job2_id, values), 0);
  }

  void TestGcsTableWithJobIdApi() {
    auto table = gcs_table_storage_->ActorTable();
    JobID job_id1 = JobID::FromInt(1);
    JobID job_id2 = JobID::FromInt(2);
    JobID job_id3 = JobID::FromInt(3);
    auto actor_table_data1 = Mocker::GenActorTableData(job_id1);
    auto actor_table_data2 = Mocker::GenActorTableData(job_id2);
    auto actor_table_data3 = Mocker::GenActorTableData(job_id3);
    ActorID actor_id1 = ActorID::FromBinary(actor_table_data1->actor_id());
    ActorID actor_id2 = ActorID::FromBinary(actor_table_data2->actor_id());
    ActorID actor_id3 = ActorID::FromBinary(actor_table_data3->actor_id());

    // Put.
    Put(table, actor_id1, *actor_table_data1);
    Put(table, actor_id2, *actor_table_data2);
    Put(table, actor_id3, *actor_table_data3);

    // Get.
    std::vector<rpc::ActorTableData> values;
    ASSERT_EQ(Get(table, actor_id1, values), 1);

    // Get by job id.
    ASSERT_EQ(GetByJobId(table, job_id1, actor_id1, values), 1);

    // Delete.
    Delete(table, actor_id1);
    ASSERT_EQ(Get(table, actor_id1, values), 0);
    ASSERT_EQ(GetByJobId(table, job_id1, actor_id1, values), 0);

    std::vector<ActorID> keys;
    keys.push_back(actor_id2);
    keys.push_back(actor_id3);
    BatchDelete(table, keys);

    ASSERT_EQ(Get(table, actor_id2, values), 0);
    ASSERT_EQ(GetByJobId(table, job_id2, actor_id2, values), 0);

    ASSERT_EQ(Get(table, actor_id3, values), 0);
    ASSERT_EQ(GetByJobId(table, job_id3, actor_id3, values), 0);
  }

  template <typename TABLE, typename KEY, typename VALUE>
  void Put(TABLE &table, const KEY &key, const VALUE &value) {
    auto on_done = [this](const Status &status) { --pending_count_; };
    ++pending_count_;
    RAY_CHECK_OK(table.Put(key, value, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int Get(TABLE &table, const KEY &key, std::vector<VALUE> &values) {
    auto on_done = [this, &values](const Status &status,
                                   const boost::optional<VALUE> &result) {
      RAY_CHECK_OK(status);
      values.clear();
      if (result) {
        values.push_back(*result);
      }
      // NOTE: The callback is executed in an asynchronous thread, so the modification of
      // pending_count_ must be put last, otherwise the unmodified pending_count_ will be
      // read outside.
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Get(key, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int GetByJobId(TABLE &table,
                 const JobID &job_id,
                 const KEY &key,
                 std::vector<VALUE> &values) {
    auto on_done = [this, &values](const absl::flat_hash_map<KEY, VALUE> &result) {
      values.clear();
      if (!result.empty()) {
        for (auto &item : result) {
          values.push_back(item.second);
        }
      }
      // NOTE: The callback is executed in an asynchronous thread, so the modification of
      // pending_count_ must be put last, otherwise the unmodified pending_count_ will be
      // read outside.
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.GetByJobId(job_id, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY>
  void Delete(TABLE &table, const KEY &key) {
    auto on_done = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Delete(key, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY>
  void BatchDelete(TABLE &table, const std::vector<KEY> &keys) {
    auto on_done = [this](const Status &status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.BatchDelete(keys, on_done));
    WaitPendingDone();
  }

  void WaitPendingDone() { WaitPendingDone(pending_count_); }

  void WaitPendingDone(std::atomic<int> &pending_count) {
    auto condition = [&pending_count]() { return pending_count == 0; };
    EXPECT_TRUE(WaitForCondition(condition, wait_pending_timeout_.count()));
  }

 protected:
  size_t io_service_num_{2};
  std::shared_ptr<IOServicePool> io_service_pool_;

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{5000};
};

}  // namespace gcs

}  // namespace ray
