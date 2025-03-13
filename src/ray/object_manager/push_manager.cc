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

#include <memory>
#include <string>
#include <utility>

#include "ray/common/common_protocol.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

void PushManager::StartPush(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  auto push_id = std::make_pair(dest_id, obj_id);
  RAY_CHECK(num_chunks > 0);

  auto it = push_info_.find(push_id);
  if (it == push_info_.end()) {
    chunks_remaining_ += num_chunks;
    auto push_state = std::make_unique<PushState>(num_chunks, send_chunk_fn);
    push_requests_with_chunks_to_send_.push_back(
        std::make_pair(push_id, push_state.get()));
    push_info_[push_id] = std::move(push_state);
  } else {
    RAY_LOG(DEBUG) << "Duplicate push request " << push_id.first << ", " << push_id.second
                   << ", resending all the chunks.";
    if (it->second->NoChunksToSend()) {
      // if all the chunks have been sent, the push request needs to be re-added to
      // `push_requests_with_chunks_to_send_`.
      push_requests_with_chunks_to_send_.push_back(
          std::make_pair(push_id, it->second.get()));
    }
    chunks_remaining_ += it->second->ResendAllChunks(send_chunk_fn);
  }
  ScheduleRemainingPushes();
}

void PushManager::OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id) {
  auto push_id = std::make_pair(dest_id, obj_id);
  chunks_in_flight_ -= 1;
  chunks_remaining_ -= 1;
  push_info_[push_id]->OnChunkComplete();
  if (push_info_[push_id]->AllChunksComplete()) {
    push_info_.erase(push_id);
    RAY_LOG(DEBUG) << "Push for " << push_id.first << ", " << push_id.second
                   << " completed, remaining: " << NumPushesInFlight();
  }
  ScheduleRemainingPushes();
}

void PushManager::ScheduleRemainingPushes() {
  bool keep_looping = true;
  // Loop over all active pushes for approximate round-robin prioritization.
  // TODO(ekl) this isn't the best implementation of round robin, we should
  // consider tracking the number of chunks active per-push and balancing those.
  while (chunks_in_flight_ < max_chunks_in_flight_ && keep_looping) {
    // Loop over each active push and try to send another chunk.
    auto it = push_requests_with_chunks_to_send_.begin();
    keep_looping = false;
    while (it != push_requests_with_chunks_to_send_.end() &&
           chunks_in_flight_ < max_chunks_in_flight_) {
      auto push_id = it->first;
      auto &info = it->second;
      if (info->SendOneChunk()) {
        chunks_in_flight_ += 1;
        keep_looping = true;
        RAY_LOG(DEBUG) << "Sending chunk " << info->next_chunk_id << " of "
                       << info->num_chunks << " for push " << push_id.first << ", "
                       << push_id.second << ", chunks in flight " << NumChunksInFlight()
                       << " / " << max_chunks_in_flight_
                       << " max, remaining chunks: " << NumChunksRemaining();
      }
      if (info->NoChunksToSend()) {
        it = push_requests_with_chunks_to_send_.erase(it);
      } else {
        it++;
      }
    }
  }
}

void PushManager::RecordMetrics() const {
  ray::stats::STATS_push_manager_in_flight_pushes.Record(NumPushesInFlight());
  ray::stats::STATS_push_manager_chunks.Record(NumChunksInFlight(), "InFlight");
  ray::stats::STATS_push_manager_chunks.Record(NumChunksRemaining(), "Remaining");
}

std::string PushManager::DebugString() const {
  std::stringstream result;
  result << "PushManager:";
  result << "\n- num pushes in flight: " << NumPushesInFlight();
  result << "\n- num chunks in flight: " << NumChunksInFlight();
  result << "\n- num chunks remaining: " << NumChunksRemaining();
  result << "\n- max chunks allowed: " << max_chunks_in_flight_;
  return result.str();
}

}  // namespace ray
