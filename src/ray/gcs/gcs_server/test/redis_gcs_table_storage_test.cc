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
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

class GcsTableStorageTest : public gcs::StoreClientTestBase {
 public:
  GcsTableStorageTest() {}

  virtual ~GcsTableStorageTest() {}

  static void SetUpTestCase() { RedisServiceManagerForTest::SetUpTestCase(); }

  static void TearDownTestCase() { RedisServiceManagerForTest::TearDownTestCase(); }

  void InitStoreClient() override {
    gcs::RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<gcs::RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

 protected:
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

  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

TEST_F(GcsTableStorageTest, TestGcsTableApi) {
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
  ASSERT_EQ(Get(table, job1_id, values), 0);
  ASSERT_EQ(Get(table, job2_id, values), 1);
}

TEST_F(GcsTableStorageTest, TestGcsTableWithJobIdApi) {
  auto table = gcs_table_storage_->ActorTable();
  JobID job_id = JobID::FromInt(3);
  auto actor_table_data = Mocker::GenActorTableData(job_id);
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
