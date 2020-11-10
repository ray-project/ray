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

#include "ray/common/common_protocol.h"
#include "ray/util/util.h"

namespace ray {

void PushManager::StartPush(const NodeID &dest_id, const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  auto push_id = std::make_pair(dest_id, obj_id);
  if (push_info_.contains(push_id)) {
    RAY_LOG(DEBUG) << "Duplicate push request " << push_id.first << ", "
                   << push_id.second;
    return;
  }
  RAY_CHECK(num_chunks > 0);
  push_info_[push_id] = std::make_pair(num_chunks, send_chunk_fn);
  next_chunk_id_[push_id] = 0;
  chunks_remaining_[push_id] += num_chunks;
  ScheduleRemainingPushes();
  RAY_CHECK(push_info_.size() == next_chunk_id_.size());
}

void PushManager::OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id) {
  auto push_id = std::make_pair(dest_id, obj_id);
  chunks_in_flight_ -= 1;
  if (--chunks_remaining_[push_id] <= 0) {
    next_chunk_id_.erase(push_id);
    chunks_remaining_.erase(push_id);
    push_info_.erase(push_id);
    RAY_LOG(DEBUG) << "Push for " << push_id.first << ", " << push_id.second
                   << " completed, remaining: " << NumPushesInFlight();
  }
  ScheduleRemainingPushes();
}

void PushManager::ScheduleRemainingPushes() {
  bool remaining = true;
  // Loop over all active pushes for approximate round-robin prioritization.
  // TODO(ekl) this isn't the best implementation of round robin, we should
  // consider tracking the number of chunks active per-push and balancing those.
  while (chunks_in_flight_ < max_chunks_in_flight_ && remaining) {
    // Loop over each active push and try to send another chunk.
    auto it = push_info_.begin();
    remaining = false;
    while (it != push_info_.end() && chunks_in_flight_ < max_chunks_in_flight_) {
      auto push_id = it->first;
      auto max_chunks = it->second.first;
      auto send_chunk_fn = it->second.second;
      if (next_chunk_id_[push_id] < max_chunks) {
        // Send the next chunk for this push.
        send_chunk_fn(next_chunk_id_[push_id]++);
        chunks_in_flight_ += 1;
        remaining = true;
        RAY_LOG(DEBUG) << "Sending chunk " << next_chunk_id_[push_id] << " of "
                       << max_chunks << " for push " << push_id.first << ", "
                       << push_id.second << ", chunks in flight " << NumChunksInFlight()
                       << " / " << max_chunks_in_flight_
                       << " max, remaining chunks: " << NumChunksRemaining();
      }
      it++;
    }
  }
}

}  // namespace ray
