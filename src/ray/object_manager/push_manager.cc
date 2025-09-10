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

#include <string>
#include <utility>

#include "ray/stats/metric_defs.h"

namespace ray {

void PushManager::StartPush(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  auto push_id = std::make_pair(dest_id, obj_id);
  RAY_CHECK(num_chunks > 0);

  auto &dest_map = push_state_map_[dest_id];
  auto it = dest_map.find(obj_id);
  if (it == dest_map.end()) {
    chunks_remaining_ += num_chunks;
    dest_map[obj_id] = push_requests_with_chunks_to_send_.emplace(
        push_requests_with_chunks_to_send_.end(),
        dest_id,
        obj_id,
        num_chunks,
        std::move(send_chunk_fn));
  } else {
    RAY_LOG(DEBUG) << "Duplicate push request " << push_id.first << ", " << push_id.second
                   << ", resending all the chunks.";
    RAY_CHECK_NE(it->second->num_chunks_to_send_, 0);
    chunks_remaining_ += it->second->ResendAllChunks(std::move(send_chunk_fn));
  }
  ScheduleRemainingPushes();
}

void PushManager::OnChunkComplete() {
  chunks_in_flight_ -= 1;
  chunks_remaining_ -= 1;
  ScheduleRemainingPushes();
}

void PushManager::ScheduleRemainingPushes() {
  // TODO(ekl) this isn't the best implementation of round robin, we should
  // consider tracking the number of chunks active per-push and balancing those.
  // TODO(dayshah): Does round-robin even make sense here? We should probably finish
  // pushes in the order they were asked for, so that some finish and some work can start.
  // Otherwise all work will be halted for a period of time

  // Loop over all active pushes for approximate round-robin prioritization.
  bool keep_looping = true;
  while (chunks_in_flight_ < max_chunks_in_flight_ && keep_looping) {
    // Loop over each active push and try to send another chunk.
    // If we could push out a chunk and haven't reached the chunks_in_flight_ limit,
    // we'll loop again to try to send more chunks.
    keep_looping = false;
    auto iter = push_requests_with_chunks_to_send_.begin();
    while (iter != push_requests_with_chunks_to_send_.end() &&
           chunks_in_flight_ < max_chunks_in_flight_) {
      auto &push_state = *iter;
      push_state.SendOneChunk();
      chunks_in_flight_ += 1;
      if (push_state.num_chunks_to_send_ == 0) {
        auto push_state_map_iter = push_state_map_.find(push_state.node_id_);
        RAY_CHECK(push_state_map_iter != push_state_map_.end());

        auto &dest_map = push_state_map_iter->second;
        auto dest_map_iter = dest_map.find(push_state.object_id_);
        RAY_CHECK(dest_map_iter != dest_map.end());

        iter = push_requests_with_chunks_to_send_.erase(dest_map_iter->second);
        dest_map.erase(dest_map_iter);
        if (dest_map.empty()) {
          push_state_map_.erase(push_state_map_iter);
        }
      } else {
        keep_looping = true;
        iter++;
      }
    }
  }
}

void PushManager::HandleNodeRemoved(const NodeID &node_id) {
  auto push_state_map_iter = push_state_map_.find(node_id);
  if (push_state_map_iter == push_state_map_.end()) {
    return;
  }
  for (auto &[_, push_state_iter] : push_state_map_iter->second) {
    push_requests_with_chunks_to_send_.erase(push_state_iter);
  }
  push_state_map_.erase(node_id);
}

void PushManager::RecordMetrics() const {
  ray::stats::STATS_push_manager_num_pushes_remaining.Record(
      NumPushRequestsWithChunksToSend());
  ray::stats::STATS_push_manager_chunks.Record(NumChunksInFlight(), "InFlight");
  ray::stats::STATS_push_manager_chunks.Record(NumChunksRemaining(), "Remaining");
}

std::string PushManager::DebugString() const {
  std::stringstream result;
  result << "PushManager:";
  result << "\n- num pushes remaining: " << NumPushRequestsWithChunksToSend();
  result << "\n- num chunks in flight: " << NumChunksInFlight();
  result << "\n- num chunks remaining: " << NumChunksRemaining();
  result << "\n- max chunks allowed: " << max_chunks_in_flight_;
  return result.str();
}

}  // namespace ray
