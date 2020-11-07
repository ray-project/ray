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

#include "ray/object_manager/object_manager.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

TEST(TestPushManager, TestSingleTransfer) {
  std::vector<int> results;
  results.reserve(10);
  UniqueID push_id = UniqueID::FromRandom();
  PushManager pm(5);
  pm.StartPush(push_id, 10, [&](int64_t chunk_id) {
    results[chunk_id] = 1;
  });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 5);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  for (int i=0; i < 10; i++) {
    pm.OnChunkComplete();
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
  for (int i=0; i < 10; i++) {
    ASSERT_EQ(results[i], 1);
  }
}

TEST(TestPushManager, TestMultipleTransfers) {
  std::vector<int> results1;
  results1.reserve(10);
  std::vector<int> results2;
  results2.reserve(10);
  UniqueID push1 = UniqueID::FromRandom();
  UniqueID push2 = UniqueID::FromRandom();
  PushManager pm(5);
  pm.StartPush(push1, 10, [&](int64_t chunk_id) {
    results1[chunk_id] = 1;
  });
  pm.StartPush(push2, 10, [&](int64_t chunk_id) {
    results2[chunk_id] = 2;
  });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 15);
  ASSERT_EQ(pm.NumPushesInFlight(), 2);
  for (int i=0; i < 20; i++) {
    pm.OnChunkComplete();
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
  for (int i=0; i < 10; i++) {
    ASSERT_EQ(results1[i], 1);
  }
  for (int i=0; i < 10; i++) {
    ASSERT_EQ(results2[i], 2);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
