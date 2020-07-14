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
#include "ray/gcs/gcs_server/test/gcs_table_storage_test_base.h"
#include "ray/gcs/store_client/redis_store_client.h"

namespace ray {

class RedisGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    gcs::RedisClientOptions options("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "",
                                    true);
    redis_client_ = std::make_shared<gcs::RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);

    thread_num_ = 10;
  }

  void TearDown() override { redis_client_->Disconnect(); }

  void TestGcsTableApiWithMultiThread() {
    std::vector<std::thread> threads;
    threads.reserve(thread_num_);

    auto test_func = [this](int i) {
      auto table = gcs_table_storage_->JobTable();
      JobID job_id = JobID::FromInt(i);
      auto job1_table_data = Mocker::GenJobTableData(job_id);

      // Put.
      PutForMultiThread(table, job_id, *job1_table_data);

      // Get.
      std::vector<rpc::JobTableData> values;
      ASSERT_EQ(GetForMultiThread(table, job_id, values), 1);

      // Delete.
      DeleteForMultiThread(table, job_id);
      ASSERT_EQ(GetForMultiThread(table, job_id, values), 0);
    };

    for (int i = 0; i < thread_num_; ++i) {
      threads.emplace_back(test_func, i);
    }

    for (int i = 0; i < thread_num_; ++i) {
      threads[i].join();
    }
  }

  void TestGcsTableWithJobIdApiWithMultiThread() {
    std::vector<std::thread> threads;
    threads.reserve(thread_num_);

    auto test_func = [this](int i) {
      auto table = gcs_table_storage_->ActorTable();
      JobID job_id = JobID::FromInt(i);
      auto actor_table_data = Mocker::GenActorTableData(job_id);
      ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

      // Put.
      PutForMultiThread(table, actor_id, *actor_table_data);

      // Get.
      std::vector<rpc::ActorTableData> values;
      ASSERT_EQ(GetForMultiThread(table, actor_id, values), 1);

      // Get by job id.
      ASSERT_EQ(GetByJobIdForMultiThread(table, job_id, actor_id, values), 1);

      // Delete.
      DeleteForMultiThread(table, actor_id);
      ASSERT_EQ(GetForMultiThread(table, actor_id, values), 0);
    };

    for (int i = 0; i < thread_num_; ++i) {
      threads.emplace_back(test_func, i);
    }

    for (int i = 0; i < thread_num_; ++i) {
      threads[i].join();
    }
  }

 protected:
  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::int64_t thread_num_;
};

TEST_F(RedisGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApi) { TestGcsTableWithJobIdApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableApiWithMultiThread) {
  TestGcsTableApiWithMultiThread();
}

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApiWithMultiThread) {
  TestGcsTableWithJobIdApiWithMultiThread();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
