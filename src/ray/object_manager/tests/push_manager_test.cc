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
  PushManager pm(5);
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete();
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 1);
  }
}

TEST(TestPushManager, TestPushState) {
  // normal sending.
  {
    std::vector<int64_t> sent_chunks;
    PushManager::PushState state{
        NodeID::FromRandom(), ObjectID::FromRandom(), 2, [&](int64_t chunk_id) {
          sent_chunks.push_back(chunk_id);
        }};
    ASSERT_EQ(state.num_chunks_, 2);
    ASSERT_EQ(state.next_chunk_id_, 0);
    ASSERT_EQ(state.num_chunks_to_send_, 2);

    state.SendOneChunk();
    ASSERT_EQ(state.num_chunks_, 2);
    ASSERT_EQ(state.next_chunk_id_, 1);
    ASSERT_EQ(state.num_chunks_to_send_, 1);
    ASSERT_EQ(sent_chunks, (std::vector<int64_t>{0}));

    state.SendOneChunk();
    ASSERT_EQ(state.num_chunks_, 2);
    ASSERT_EQ(state.next_chunk_id_, 0);
    ASSERT_EQ(state.num_chunks_to_send_, 0);
    ASSERT_EQ(sent_chunks, (std::vector<int64_t>{0, 1}));
    ASSERT_EQ(state.num_chunks_to_send_, 0);
  }

  // resend all chunks.
  {
    std::vector<int64_t> sent_chunks;
    PushManager::PushState state{
        NodeID::FromRandom(), ObjectID::FromRandom(), 3, [&](int64_t chunk_id) {
          sent_chunks.push_back(chunk_id);
        }};
    state.SendOneChunk();
    ASSERT_EQ(state.num_chunks_, 3);
    ASSERT_EQ(state.next_chunk_id_, 1);
    ASSERT_EQ(state.num_chunks_to_send_, 2);
    ASSERT_EQ(sent_chunks, (std::vector<int64_t>{0}));

    // resend chunks when 1 chunk is in flight.
    ASSERT_EQ(1, state.ResendAllChunks([&](int64_t chunk_id) {
      sent_chunks.push_back(chunk_id);
    }));
    ASSERT_EQ(state.num_chunks_, 3);
    ASSERT_EQ(state.next_chunk_id_, 1);
    ASSERT_EQ(state.num_chunks_to_send_, 3);

    for (auto i = 0; i < 3; i++) {
      state.SendOneChunk();
      ASSERT_EQ(state.num_chunks_, 3);
      ASSERT_EQ(state.next_chunk_id_, (2 + i) % 3);
      ASSERT_EQ(state.num_chunks_to_send_, 3 - i - 1);
    }

    ASSERT_EQ(sent_chunks, (std::vector<int64_t>{0, 1, 2, 0}));
    ASSERT_EQ(state.num_chunks_to_send_, 0);
  }
}

TEST(TestPushManager, TestRetryDuplicates) {
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm(5);

  // First push request.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  // Second push request will resent the full chunks.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 15);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  // first 5 chunks will be sent by first push request.
  for (int i = 0; i < 5; i++) {
    pm.OnChunkComplete();
  }
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(results[i], 1);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  // we will resend all chunks by second push request.
  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete();
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 2);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
}

TEST(TestPushManager, TestResendWholeObject) {
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm(5);
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);

  for (int i = 0; i < 5; i++) {
    pm.OnChunkComplete();
  }
  // All chunks have been sent out
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 5);

  // resend this object, and it needs to be added to the traversal list.
  pm.StartPush(node_id, obj_id, 10, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 15);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  // we will resend all chunks by second push request.
  for (int i = 0; i < 15; i++) {
    pm.OnChunkComplete();
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
  int num_active1 = 0;
  int num_active2 = 0;
  PushManager pm(5);
  pm.StartPush(node1, obj_id, 10, [&](int64_t chunk_id) {
    results1[chunk_id] = 1;
    num_active1++;
  });
  pm.StartPush(node2, obj_id, 10, [&](int64_t chunk_id) {
    results2[chunk_id] = 2;
    num_active2++;
  });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 20);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 2);
  for (int i = 0; i < 20; i++) {
    if (num_active1 > 0) {
      pm.OnChunkComplete();
      num_active1--;
    } else if (num_active2 > 0) {
      pm.OnChunkComplete();
      num_active2--;
    }
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
  PushManager pm(3);

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
  ASSERT_EQ(pm.NumChunksInFlight(), 3);
  ASSERT_EQ(pm.NumChunksRemaining(), 7);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 3);

  pm.OnChunkComplete();
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 2);
  pm.OnChunkComplete();
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  pm.OnChunkComplete();
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  pm.OnChunkComplete();
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);

  pm.OnChunkComplete();
  pm.OnChunkComplete();
  pm.OnChunkComplete();

  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);

  ASSERT_EQ(result[obj_id_1].size(), 4);
  ASSERT_EQ(result[obj_id_2].size(), 1);
  ASSERT_EQ(result[obj_id_3].size(), 2);
}

TEST(TestPushManager, TestNodeRemoved) {
  PushManager pm(3);

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

  // 3 chunks in flight for 3 objects to two nodes.
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 3);
  ASSERT_EQ(pm.NumChunksInFlight(), 3);
  ASSERT_EQ(pm.push_state_map_.size(), 2);
  ASSERT_EQ(pm.push_requests_with_chunks_to_send_.size(), 3);

  // Remove Node 1. This should cause its associated push requests to be cleaned up.
  pm.HandleNodeRemoved(node_id_1);
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 1);
  ASSERT_EQ(pm.NumChunksInFlight(), 3);
  ASSERT_EQ(pm.push_state_map_.size(), 1);
  ASSERT_EQ(pm.push_requests_with_chunks_to_send_.size(), 1);

  // All 3 in flight chunks finish.
  // All pushes should be done with chunks to node 2 in flight.
  for (int i = 0; i < 3; i++) {
    pm.OnChunkComplete();
  }
  ASSERT_EQ(pm.NumPushRequestsWithChunksToSend(), 0);
  ASSERT_EQ(pm.NumChunksInFlight(), 3);
  ASSERT_EQ(pm.push_state_map_.size(), 0);
  ASSERT_EQ(pm.push_requests_with_chunks_to_send_.size(), 0);

  // The in flight chunks complete.
  for (int i = 0; i < 3; i++) {
    pm.OnChunkComplete();
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
