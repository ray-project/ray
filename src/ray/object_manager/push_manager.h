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

#include <list>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"

namespace ray {

/// Manages rate limiting and deduplication of outbound object pushes.
class PushManager {
 public:
  /// Create a push manager.
  ///
  /// \param max_bytes_in_flight Max number of bytes allowed to be in flight
  ///                             from this PushManager (this raylet).
  explicit PushManager(int64_t max_bytes_in_flight)
      : max_bytes_in_flight_(max_bytes_in_flight){};

  /// Start pushing an object subject to max chunks in flight limit.
  ///
  /// Duplicate concurrent pushes to the same destination will be suppressed.
  ///
  /// \param dest_id The node to send to.
  /// \param obj_id The object to send.
  /// \param num_chunks The total number of chunks to send.
  /// \param max_chunk_size See comment for max_chunk_size_ in PushState.
  /// \param send_chunk_fn This function will be called with args 0...{num_chunks-1}.
  ///                      The caller promises to call PushManager::OnChunkComplete()
  ///                      once a call to send_chunk_fn finishes.
  void StartPush(const NodeID &dest_id,
                 const ObjectID &obj_id,
                 int64_t num_chunks,
                 int64_t max_chunk_size,
                 std::function<void(int64_t)> send_chunk_fn);

  /// Called every time a chunk completes to trigger additional sends.
  /// TODO(ekl) maybe we should cancel the entire push on error.
  void OnChunkComplete(int64_t push_max_chunk_size);

  /// Cancel all pushes that have not yet been sent to the removed node.
  void HandleNodeRemoved(const NodeID &node_id);

  void RecordMetrics() const;

  int64_t BytesInFlight() const { return bytes_in_flight_; }

  int64_t ChunksRemaining() const { return chunks_remaining_; }

  int64_t PushesInFlight() const { return push_state_map_.size(); }

  int64_t PushRequestsRemaining() const {
    return push_requests_with_chunks_to_send_.size();
  }

  std::string DebugString() const;

 private:
  FRIEND_TEST(TestPushManager, TestPushState);

  /// Tracks the state of an active object push to another node.
  struct PushState {
    NodeID node_id_;
    ObjectID object_id_;

    /// total number of chunks of this object.
    int64_t num_chunks_;
    /// the max size of a chunk for this object in bytes, used to count bytes_in_flight_
    /// and assure it stays under max_bytes_in_flight_. This means we can overcount for
    /// the last chunk but we're accepting that to keep the code simpler.
    int64_t max_chunk_size_;
    /// The function to send chunks with.
    std::function<void(int64_t)> chunk_send_fn_;

    /// The index of the next chunk to send.
    int64_t next_chunk_id_ = 0;
    /// The number of chunks remaining to send.
    int64_t num_chunks_to_send_;

    PushState(NodeID node_id,
              ObjectID object_id,
              int64_t num_chunks,
              int64_t max_chunk_size,
              std::function<void(int64_t)> chunk_send_fn)
        : node_id_(node_id),
          object_id_(object_id),
          num_chunks_(num_chunks),
          max_chunk_size_(max_chunk_size),
          chunk_send_fn_(std::move(chunk_send_fn)),
          num_chunks_to_send_(num_chunks) {}

    /// Resend all chunks and returns how many more chunks will be sent.
    int64_t ResendAllChunks(std::function<void(int64_t)> send_fn) {
      chunk_send_fn_ = std::move(send_fn);
      int64_t additional_chunks_to_send = num_chunks_ - num_chunks_to_send_;
      num_chunks_to_send_ = num_chunks_;
      return additional_chunks_to_send;
    }

    /// Send one chunk. Return true if a new chunk is sent, false if no more chunk to
    /// send.
    void SendOneChunk() {
      num_chunks_to_send_--;
      // Send the next chunk for this push.
      chunk_send_fn_(next_chunk_id_);
      next_chunk_id_ = (next_chunk_id_ + 1) % num_chunks_;
    }
  };

  /// Called on completion events to trigger additional pushes.
  void ScheduleRemainingPushes();

  /// Max number of bytes in flight allowed.
  const int64_t max_bytes_in_flight_;

  /// Running count of bytes in flight
  int64_t bytes_in_flight_ = 0;

  /// Remaining count of chunks to push to other nodes.
  int64_t chunks_remaining_ = 0;

  /// Tracks all pushes with chunk transfers in flight.
  absl::flat_hash_map<NodeID,
                      absl::flat_hash_map<ObjectID, std::list<PushState>::iterator>>
      push_state_map_;

  /// The list of push requests with chunks waiting to be sent.
  std::list<PushState> push_requests_with_chunks_to_send_;
};

}  // namespace ray
