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

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/object_manager/metrics.h"

namespace ray {

/// Manages deduplication of outbound object pushes.
class PushManager {
 public:
  PushManager() = default;

  /// Start pushing an object. All chunks are scheduled immediately.
  ///
  /// Duplicate concurrent pushes to the same destination will be suppressed.
  ///
  /// \param dest_id The node to send to.
  /// \param obj_id The object to send.
  /// \param num_chunks The total number of chunks to send.
  /// \param send_chunk_fn This function will be called with args 0...{num_chunks-1}.
  ///                      The caller promises to call PushManager::OnChunkComplete()
  ///                      once a call to send_chunk_fn finishes.
  void StartPush(const NodeID &dest_id,
                 const ObjectID &obj_id,
                 int64_t num_chunks,
                 std::function<void(int64_t)> send_chunk_fn);

  /// Called every time a chunk completes. Cleans up the push entry when all
  /// chunks for that push have finished.
  void OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id);

  /// Cancel all pushes that have not yet been sent to the removed node.
  void HandleNodeRemoved(const NodeID &node_id);

  /// Return the number of chunks currently in flight. For metrics and testing.
  int64_t NumChunksInFlight() const { return chunks_in_flight_; };

  /// Return the number of chunks remaining. For metrics and testing.
  int64_t NumChunksRemaining() const { return chunks_remaining_; }

  /// Return the number of active pushes (individual object-to-node transfers).
  /// For metrics and testing.
  int64_t NumPushRequestsWithChunksToSend() const {
    int64_t count = 0;
    for (const auto &[_, dest_map] : active_pushes_) {
      count += dest_map.size();
    }
    return count;
  }

  /// Record the internal metrics.
  void RecordMetrics() const;

  std::string DebugString() const;

 private:
  FRIEND_TEST(TestPushManager, TestPushState);
  FRIEND_TEST(TestPushManager, TestNodeRemoved);

  /// Tracks the state of an active object push to another node.
  struct PushState {
    NodeID node_id_;
    ObjectID object_id_;
    int64_t num_chunks_;
    int64_t chunks_remaining_;
  };

  /// Running count of chunks in flight.
  int64_t chunks_in_flight_ = 0;

  /// Remaining count of chunks to push to other nodes.
  int64_t chunks_remaining_ = 0;

  /// Tracks all active pushes for deduplication.
  /// Maps (dest_id, obj_id) -> PushState.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<ObjectID, PushState>> active_pushes_;

  mutable ray::stats::Gauge push_manager_num_pushes_remaining_gauge_{
      GetPushManagerNumPushesRemainingGaugeMetric()};
  mutable ray::stats::Gauge push_manager_chunks_gauge_{GetPushManagerChunksGaugeMetric()};
};

}  // namespace ray
