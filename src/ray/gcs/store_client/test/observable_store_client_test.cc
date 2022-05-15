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

#include "ray/gcs/store_client/observable_store_client.h"

#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class ObservableStoreClientTest : public StoreClientTestBase {
 public:
  void InitStoreClient() override {
    store_client_ = std::make_shared<ObservableStoreClient>(
        std::make_unique<InMemoryStoreClient>(*(io_service_pool_->Get())));
  }

  void DisconnectStoreClient() override {}
};

TEST_F(ObservableStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(ObservableStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
