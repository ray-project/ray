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
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {

class InMemoryGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  InMemoryGcsTableStorageTest() {
    io_service_pool_ = std::make_shared<IOServicePool>(io_service_num_);
    io_service_pool_->Run();
  }

  virtual ~InMemoryGcsTableStorageTest() { io_service_pool_->Stop(); }

  static void SetUpTestCase() { RedisServiceManagerForTest::SetUpTestCase(); }

  static void TearDownTestCase() { RedisServiceManagerForTest::TearDownTestCase(); }

  void InitTableStorage() override {
    gcs_table_storage_ =
        std::make_shared<gcs::InMemoryGcsTableStorage>(*(io_service_pool_->Get()));
  }

  void DeInitTableStorage() override {}

 protected:
  size_t io_service_num_{3};
  std::shared_ptr<IOServicePool> io_service_pool_;
};

TEST_F(InMemoryGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(InMemoryGcsTableStorageTest, TestGcsTableWithJobIdApi) {
  TestGcsTableWithJobIdApi();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
