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

#include "ray/object_manager/push_manager.h"

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

namespace ray {

TEST(TestPushManager, TestSingleTransfer) {
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm;
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  // All chunks scheduled immediately.
  ASSERT_EQ(pm.NumChunksInFlight(), 10);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 1);
  }
}

TEST(TestPushManager, TestPushState) {
  // Verify PushState tracks chunks_remaining correctly.
  {
    auto node_id = NodeID::FromRandom();
    auto obj_id = ObjectID::FromRandom();
    PushManager pm;
    int send_count = 0;
    pm.StartPush(node_id, obj_id, 3, [&](int64_t chunk_id) { send_count++; });
    ASSERT_EQ(send_count, 3);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  }

  // Verify duplicate push extends chunk tracking.
  {
    auto node_id = NodeID::FromRandom();
    auto obj_id = ObjectID::FromRandom();
    PushManager pm;
    int send_count = 0;
    pm.StartPush(node_id, obj_id, 2, [&](int64_t chunk_id) { send_count++; });
    ASSERT_EQ(send_count, 2);

    // Complete one chunk, then resend.
    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

    // Duplicate push resends all chunks.
    pm.StartPush(node_id, obj_id, 2, [&](int64_t chunk_id) { send_count++; });
    ASSERT_EQ(send_count, 4);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

    // Need to complete remaining original chunk (1) + resent chunks (2) = 3.
    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
    pm.OnChunkComplete(node_id, obj_id);
    ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  }
}

TEST(TestPushManager, TestRetryDuplicates) {
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm;

  // First push request.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 10);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  // Second push request will resend all chunks.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksInFlight(), 20);
  ASSERT_EQ(pm.NumChunksRemaining(), 20);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  // Complete all 20 chunks (10 original + 10 resent).
  for (int i = 0; i < 20; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 2);
  }
}

TEST(TestPushManager, TestResendWholeObject) {
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm;
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 10);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  for (int i = 0; i < 5; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  ASSERT_EQ(pm.NumChunksRemaining(), 5);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  // Resend this object — adds more chunks to track.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksRemaining(), 15);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  // Complete all remaining chunks.
  for (int i = 0; i < 15; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 2);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
}

TEST(TestPushManager, TestMultipleTransfers) {
  std::vector<int> results1;
  results1.resize(10);
  std::vector<int> results2;
  results2.resize(10);
  auto node1 = NodeID::FromRandom();
  auto node2 = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm;
  pm.StartPush(node1, obj_id, 10, [&](int64_t chunk_id) { results1[chunk_id] = 1; });
  pm.StartPush(node2, obj_id, 10, [&](int64_t chunk_id) { results2[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksInFlight(), 20);
  ASSERT_EQ(pm.NumChunksRemaining(), 20);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 2);

  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete(node1, obj_id);
  }
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete(node2, obj_id);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results1[i], 1);
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results2[i], 2);
  }
}

TEST(TestPushManager, TestPushMultipleObject) {
  auto node_id = NodeID::FromRandom();
  auto obj_id_1 = ObjectID::FromRandom();
  auto obj_id_2 = ObjectID::FromRandom();
  auto obj_id_3 = ObjectID::FromRandom();
  PushManager pm;

  absl::flat_hash_map<ObjectID, absl::flat_hash_set<int64_t>> result;
  pm.StartPush(node_id, obj_id_1, 4, [&, obj_id = obj_id_1](int64_t chunk_id) {
    ASSERT_FALSE(result[obj_id].contains(chunk_id));
    result[obj_id].insert(chunk_id);
  });
  pm.StartPush(node_id, obj_id_2, 1, [&, obj_id = obj_id_2](int64_t chunk_id) {
    ASSERT_FALSE(result[obj_id].contains(chunk_id));
    result[obj_id].insert(chunk_id);
  });
  pm.StartPush(node_id, obj_id_3, 2, [&, obj_id = obj_id_3](int64_t chunk_id) {
    ASSERT_FALSE(result[obj_id].contains(chunk_id));
    result[obj_id].insert(chunk_id);
  });
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 3);
  ASSERT_EQ(pm.NumChunksInFlight(), 7);
  ASSERT_EQ(pm.NumChunksRemaining(), 7);

  // Complete the 1-chunk object.
  pm.OnChunkComplete(node_id, obj_id_2);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 2);

  // Complete the 2-chunk object.
  pm.OnChunkComplete(node_id, obj_id_3);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 2);
  pm.OnChunkComplete(node_id, obj_id_3);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  // Complete the 4-chunk object.
  for (int i = 0; i < 4; i++) {
    pm.OnChunkComplete(node_id, obj_id_1);
  }
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);

  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);

  ASSERT_EQ(result[obj_id_1].size(), 4);
  ASSERT_EQ(result[obj_id_2].size(), 1);
  ASSERT_EQ(result[obj_id_3].size(), 2);
}

TEST(TestPushManager, TestNodeRemoved) {
  PushManager pm;

  // Start pushing two objects to node 1.
  auto node_id_1 = NodeID::FromRandom();
  auto obj_id_1 = ObjectID::FromRandom();
  auto obj_id_2 = ObjectID::FromRandom();
  pm.StartPush(node_id_1, obj_id_1, 4, [](int64_t) {});
  pm.StartPush(node_id_1, obj_id_2, 2, [](int64_t) {});

  // Start pushing one object to node 2.
  auto node_id_2 = NodeID::FromRandom();
  auto obj_id_3 = ObjectID::FromRandom();
  pm.StartPush(node_id_2, obj_id_3, 3, [](int64_t) {});

  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 3);
  ASSERT_EQ(pm.NumChunksInFlight(), 9);
  ASSERT_EQ(pm.active_pushes_.size(), 2);

  // Remove Node 1. This cleans up its push entries.
  pm.HandleNodeRemoved(node_id_1);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  ASSERT_EQ(pm.active_pushes_.size(), 1);
  // In-flight chunks for removed node still decrement when they complete.
  ASSERT_EQ(pm.NumChunksInFlight(), 9);

  // Complete node 1's in-flight chunks (already removed from active_pushes_).
  for (int i = 0; i < 6; i++) {
    pm.OnChunkComplete(node_id_1, (i < 4) ? obj_id_1 : obj_id_2);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 3);

  // Complete node 2's chunks.
  for (int i = 0; i < 3; i++) {
    pm.OnChunkComplete(node_id_2, obj_id_3);
  }
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.active_pushes_.size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
