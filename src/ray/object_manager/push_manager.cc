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
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

void PushManager::StartPush(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  RAY_CHECK(num_chunks > 0);
  if (push_info_.contains(dest_id) && push_info_[dest_id].first.contains(obj_id)) {
    RAY_LOG(DEBUG) << "Duplicate push request " << dest_id << ", " << obj_id
                   << ", resending all the chunks.";
    auto push_state = push_info_[dest_id].first[obj_id];
    if (push_state->HasNoChunkRemained()) {
      push_info_[dest_id].second.push(push_state);
    }
    chunks_remaining_ += push_state->ResendAllChunks(send_chunk_fn);
  } else {
    num_pushes_in_flight_ += 1;
    chunks_remaining_ += num_chunks;
    auto push_state = std::make_shared<PushState>(num_chunks, send_chunk_fn, obj_id);
    if (push_info_.contains(dest_id)) {
      push_info_[dest_id].first[obj_id] = push_state;
      push_info_[dest_id].second.push(push_state);
    } else {
      auto pair =
          std::make_pair(absl::flat_hash_map<ObjectID, std::shared_ptr<PushState>>(),
                         std::queue<std::shared_ptr<PushState>>());
      auto it = push_info_.emplace(dest_id, std::move(pair)).first;
      it->second.first.emplace(obj_id, push_state);
      it->second.second.push(push_state);
    }
  }
  ScheduleRemainingPushes();
}

void PushManager::OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id) {
  chunks_in_flight_ -= 1;
  chunks_remaining_ -= 1;
  push_info_[dest_id].first[obj_id]->OnChunkComplete();
  if (push_info_[dest_id].first[obj_id]->AllChunksComplete()) {
    push_info_[dest_id].first.erase(obj_id);
    if (push_info_[dest_id].first.empty()) {
      RAY_CHECK(push_info_[dest_id].second.empty());
      push_info_.erase(dest_id);
      num_pushes_in_flight_ -= 1;
    }
    RAY_LOG(DEBUG) << "Push for " << dest_id << ", " << obj_id
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
    auto it = push_info_.begin();
    keep_looping = false;
    bool into_while = false;
    while (it != push_info_.end() && chunks_in_flight_ < max_chunks_in_flight_) {
      all_loop_num_ += 1.0;
      into_while = true;
      NodeID node_id = it->first;
      if (!it->second.second.empty() && chunks_in_flight_ < max_chunks_in_flight_) {
        send_chunk_num_ += 1.0;
        auto &info = it->second.second.front();
        RAY_CHECK(info->SendOneChunk());
        chunks_in_flight_ += 1;
        keep_looping = true;
        RAY_LOG(DEBUG) << "Sending chunk " << info->next_chunk_id << " of "
                       << info->num_chunks << " for push " << info->obj_id << ", "
                       << node_id << ", chunks in flight " << NumChunksInFlight() << " / "
                       << max_chunks_in_flight_
                       << " max, remaining chunks: " << NumChunksRemaining();
        if (info->HasNoChunkRemained()) it->second.second.pop();
      }

      it++;
    }
    if (!into_while) {
      all_loop_num_ += 1.0;
    }
  }
  RAY_LOG(INFO) << "Loop summary:"
                << "\n- all loop number: " << all_loop_num_
                << "\n- send chunk number: " << send_chunk_num_
                << "\n- send_chunk_num_ / all_loop_num_:" << (send_chunk_num_ / all_loop_num_);
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
