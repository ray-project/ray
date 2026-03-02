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

namespace ray {

void PushManager::StartPush(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  RAY_CHECK(num_chunks > 0);

  auto &dest_map = active_pushes_[dest_id];
  auto it = dest_map.find(obj_id);
  if (it != dest_map.end()) {
    // Duplicate push — resend all chunks.
    RAY_LOG(DEBUG) << "Duplicate push request " << dest_id << ", " << obj_id
                   << ", resending all the chunks.";
    it->second.chunks_remaining_ += num_chunks;
    chunks_remaining_ += num_chunks;
    chunks_in_flight_ += num_chunks;
    for (int64_t i = 0; i < num_chunks; i++) {
      send_chunk_fn(i);
    }
    return;
  }

  // New push — schedule all chunks immediately.
  dest_map[obj_id] = PushState{dest_id, obj_id, num_chunks, num_chunks};
  chunks_remaining_ += num_chunks;
  chunks_in_flight_ += num_chunks;
  for (int64_t i = 0; i < num_chunks; i++) {
    send_chunk_fn(i);
  }
}

void PushManager::OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id) {
  chunks_in_flight_ -= 1;
  chunks_remaining_ -= 1;

  auto dest_it = active_pushes_.find(dest_id);
  if (dest_it == active_pushes_.end()) {
    // Already cleaned up (e.g. HandleNodeRemoved was called).
    return;
  }
  auto &dest_map = dest_it->second;
  auto obj_it = dest_map.find(obj_id);
  if (obj_it == dest_map.end()) {
    return;
  }
  obj_it->second.chunks_remaining_ -= 1;
  if (obj_it->second.chunks_remaining_ <= 0) {
    dest_map.erase(obj_it);
    if (dest_map.empty()) {
      active_pushes_.erase(dest_it);
    }
  }
}

void PushManager::HandleNodeRemoved(const NodeID &node_id) {
  active_pushes_.erase(node_id);
}

void PushManager::RecordMetrics() const {
  push_manager_num_pushes_remaining_gauge_.Record(NumPushRequestsWithChunksToSend());
  push_manager_chunks_gauge_.Record(NumChunksInFlight(), {{"Type", "InFlight"}});
  push_manager_chunks_gauge_.Record(NumChunksRemaining(), {{"Type", "Remaining"}});
}

std::string PushManager::DebugString() const {
  std::stringstream result;
  result << "PushManager:";
  result << "\n- num active pushes: " << NumPushRequestsWithChunksToSend();
  result << "\n- num chunks in flight: " << NumChunksInFlight();
  result << "\n- num chunks remaining: " << NumChunksRemaining();
  return result.str();
}

}  // namespace ray
