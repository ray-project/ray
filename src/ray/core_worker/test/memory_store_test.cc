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

#include "ray/core_worker/store_provider/memory_store/memory_store.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

TEST(TestMemoryStore, TestReportUnhandledErrors) {
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext context(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  int unhandled_count = 0;

  std::shared_ptr<CoreWorkerMemoryStore> provider =
      std::make_shared<CoreWorkerMemoryStore>(
          nullptr, nullptr, nullptr, nullptr,
          [&](const RayObject &obj) { unhandled_count++; });
  RayObject obj1(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  RayObject obj2(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  auto id1 = ObjectID::FromRandom();
  auto id2 = ObjectID::FromRandom();

  // Check delete without get.
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  ASSERT_EQ(unhandled_count, 0);
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 2);
  unhandled_count = 0;

  // Check delete after get.
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj1, id2));
  provider->Get({id1}, 1, 100, context, false, &results);
  provider->GetOrPromoteToPlasma(id2);
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);

  // Check delete after async get.
  provider->GetAsync({id2}, [](std::shared_ptr<RayObject> obj) {});
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  provider->GetAsync({id1}, [](std::shared_ptr<RayObject> obj) {});
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
