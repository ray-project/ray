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

#include <gtest/gtest.h>

#include <memory>

#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/tests/gcs_table_storage_test_base.h"

namespace ray {

class InMemoryGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  void SetUp() override {
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(
        std::make_unique<gcs::InMemoryStoreClient>());
  }
};

TEST_F(InMemoryGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(InMemoryGcsTableStorageTest, TestGcsTableWithJobIdApi) {
  TestGcsTableWithJobIdApi();
}

}  // namespace ray
