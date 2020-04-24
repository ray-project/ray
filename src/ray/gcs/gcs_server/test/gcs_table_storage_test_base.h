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

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"

namespace ray {

namespace gcs {

class GcsTableStorageTestBase : public ::testing::Test {
 public:
  GcsTableStorageTestBase() {}

  virtual ~GcsTableStorageTestBase() {}

  void SetUp() override { InitTableStorage(); }

  void TearDown() override { DeInitTableStorage(); }

  virtual void InitTableStorage() = 0;

  virtual void DeInitTableStorage() = 0;

 protected:
  void TestGcsTableApi() {
    auto table = gcs_table_storage_->JobTable();
    JobID job1_id = JobID::FromInt(1);
    JobID job2_id = JobID::FromInt(2);
    auto job1_table_data = GenJobTableData(job1_id);
    auto job2_table_data = GenJobTableData(job2_id);

    // Put.
    Put(table, job1_id, *job1_table_data);
    Put(table, job2_id, *job2_table_data);

    // Get.
    std::vector<rpc::JobTableData> values;
    ASSERT_EQ(Get(table, job2_id, values), 1);
    ASSERT_EQ(Get(table, job2_id, values), 1);

    // Delete.
    Delete(table, job1_id);
    ASSERT_EQ(Get(table, job1_id, values), 0);
    ASSERT_EQ(Get(table, job2_id, values), 1);
  }

  void TestGcsTableWithJobIdApi() {
    auto table = gcs_table_storage_->ActorTable();
    JobID job_id = JobID::FromInt(3);
    auto actor_table_data = GenActorTableData(job_id);
    ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

    // Put.
    Put(table, actor_id, *actor_table_data);

    // Get.
    std::vector<rpc::ActorTableData> values;
    ASSERT_EQ(Get(table, actor_id, values), 1);

    // Delete.
    Delete(table, actor_id);
    ASSERT_EQ(Get(table, actor_id, values), 0);
  }

  template <typename TABLE, typename KEY, typename VALUE>
  void Put(TABLE &table, const KEY &key, const VALUE &value) {
    auto on_done = [this](Status status) { --pending_count_; };
    ++pending_count_;
    RAY_CHECK_OK(table.Put(key, value, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int Get(TABLE &table, const KEY &key, std::vector<VALUE> &values) {
    auto on_done = [this, &values](Status status, const boost::optional<VALUE> &result) {
      RAY_CHECK_OK(status);
      --pending_count_;
      values.clear();
      if (result) {
        values.push_back(*result);
      }
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Get(key, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY>
  void Delete(TABLE &table, const KEY &key) {
    auto on_done = [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Delete(key, on_done));
    WaitPendingDone();
  }

  void WaitPendingDone() { WaitPendingDone(pending_count_); }

  void WaitPendingDone(std::atomic<int> &pending_count) {
    auto condition = [&pending_count]() { return pending_count == 0; };
    EXPECT_TRUE(WaitForCondition(condition, wait_pending_timeout_.count()));
  }

 protected:
  std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_driver_ip_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }

  std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    return actor_table_data;
  }

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  std::atomic<int> pending_count_{0};
  std::chrono::milliseconds wait_pending_timeout_{5000};
};

}  // namespace gcs

}  // namespace ray
