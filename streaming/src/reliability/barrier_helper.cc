#include "barrier_helper.h"

#include <algorithm>

#include "util/streaming_logging.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {
StreamingStatus StreamingBarrierHelper::GetMsgIdByBarrierId(const ObjectID &q_id,
                                                            uint64_t barrier_id,
                                                            uint64_t &msg_id) {
  std::lock_guard<std::mutex> lock(global_barrier_mutex_);
  auto queue_map = global_barrier_map_.find(barrier_id);
  if (queue_map == global_barrier_map_.end()) {
    return StreamingStatus::NoSuchItem;
  }
  auto msg_id_map = queue_map->second.find(q_id);
  if (msg_id_map == queue_map->second.end()) {
    return StreamingStatus::QueueIdNotFound;
  }
  msg_id = msg_id_map->second;
  return StreamingStatus::OK;
}

void StreamingBarrierHelper::SetMsgIdByBarrierId(const ObjectID &q_id,
                                                 uint64_t barrier_id, uint64_t msg_id) {
  std::lock_guard<std::mutex> lock(global_barrier_mutex_);
  global_barrier_map_[barrier_id][q_id] = msg_id;
}

void StreamingBarrierHelper::ReleaseBarrierMapById(uint64_t barrier_id) {
  std::lock_guard<std::mutex> lock(global_barrier_mutex_);
  global_barrier_map_.erase(barrier_id);
}

void StreamingBarrierHelper::ReleaseAllBarrierMap() {
  std::lock_guard<std::mutex> lock(global_barrier_mutex_);
  global_barrier_map_.clear();
}

void StreamingBarrierHelper::MapBarrierToCheckpoint(uint64_t barrier_id,
                                                    uint64_t checkpoint) {
  std::lock_guard<std::mutex> lock(barrier_map_checkpoint_mutex_);
  barrier_checkpoint_map_[barrier_id] = checkpoint;
}

StreamingStatus StreamingBarrierHelper::GetCheckpointIdByBarrierId(
    uint64_t barrier_id, uint64_t &checkpoint_id) {
  std::lock_guard<std::mutex> lock(barrier_map_checkpoint_mutex_);
  auto checkpoint_item = barrier_checkpoint_map_.find(barrier_id);
  if (checkpoint_item == barrier_checkpoint_map_.end()) {
    return StreamingStatus::NoSuchItem;
  }

  checkpoint_id = checkpoint_item->second;
  return StreamingStatus::OK;
}

void StreamingBarrierHelper::ReleaseBarrierMapCheckpointByBarrierId(
    const uint64_t barrier_id) {
  std::lock_guard<std::mutex> lock(barrier_map_checkpoint_mutex_);
  auto it = barrier_checkpoint_map_.begin();
  while (it != barrier_checkpoint_map_.end()) {
    if (it->first <= barrier_id) {
      it = barrier_checkpoint_map_.erase(it);
    } else {
      it++;
    }
  }
}

StreamingStatus StreamingBarrierHelper::GetBarrierIdByLastMessageId(const ObjectID &q_id,
                                                                    uint64_t message_id,
                                                                    uint64_t &barrier_id,
                                                                    bool is_pop) {
  std::lock_guard<std::mutex> lock(message_id_map_barrier_mutex_);
  auto message_item = global_reversed_barrier_map_.find(message_id);
  if (message_item == global_reversed_barrier_map_.end()) {
    return StreamingStatus::NoSuchItem;
  }

  auto message_queue_item = message_item->second.find(q_id);
  if (message_queue_item == message_item->second.end()) {
    return StreamingStatus::QueueIdNotFound;
  }
  if (message_queue_item->second->empty()) {
    STREAMING_LOG(WARNING) << "[Barrier] q id => " << q_id.Hex() << ", str num => "
                           << Util::Hexqid2str(q_id.Hex()) << ", message id "
                           << message_id;
    return StreamingStatus::NoSuchItem;
  } else {
    barrier_id = message_queue_item->second->front();
    if (is_pop) {
      message_queue_item->second->pop();
    }
  }
  return StreamingStatus::OK;
}

void StreamingBarrierHelper::SetBarrierIdByLastMessageId(const ObjectID &q_id,
                                                         uint64_t message_id,
                                                         uint64_t barrier_id) {
  std::lock_guard<std::mutex> lock(message_id_map_barrier_mutex_);

  auto max_message_id_barrier = max_message_id_map_.find(q_id);
  // remove finished barrier in different last message id
  if (max_message_id_barrier != max_message_id_map_.end() &&
      max_message_id_barrier->second != message_id) {
    if (global_reversed_barrier_map_.find(max_message_id_barrier->second) !=
        global_reversed_barrier_map_.end()) {
      global_reversed_barrier_map_.erase(max_message_id_barrier->second);
    }
  }

  max_message_id_map_[q_id] = message_id;
  auto message_item = global_reversed_barrier_map_.find(message_id);
  if (message_item == global_reversed_barrier_map_.end()) {
    BarrierIdQueue temp_queue = std::make_shared<std::queue<uint64_t>>();
    temp_queue->push(barrier_id);
    global_reversed_barrier_map_[message_id][q_id] = temp_queue;
    return;
  }
  auto message_queue_item = message_item->second.find(q_id);
  if (message_queue_item != message_item->second.end()) {
    message_queue_item->second->push(barrier_id);
  } else {
    BarrierIdQueue temp_queue = std::make_shared<std::queue<uint64_t>>();
    temp_queue->push(barrier_id);
    global_reversed_barrier_map_[message_id][q_id] = temp_queue;
  }
}

void StreamingBarrierHelper::GetAllBarrier(std::vector<uint64_t> &barrier_id_vec) {
  std::transform(
      global_barrier_map_.begin(), global_barrier_map_.end(),
      std::back_inserter(barrier_id_vec),
      [](std::unordered_map<uint64_t, std::unordered_map<ObjectID, uint64_t>>::value_type
             pair) { return pair.first; });
}

bool StreamingBarrierHelper::Contains(uint64_t barrier_id) {
  return global_barrier_map_.find(barrier_id) != global_barrier_map_.end();
}

uint32_t StreamingBarrierHelper::GetBarrierMapSize() {
  return global_barrier_map_.size();
}

void StreamingBarrierHelper::GetCurrentMaxCheckpointIdInQueue(
    const ObjectID &q_id, uint64_t &checkpoint_id) const {
  auto item = current_max_checkpoint_id_map_.find(q_id);
  if (item != current_max_checkpoint_id_map_.end()) {
    checkpoint_id = item->second;
  } else {
    checkpoint_id = 0;
  }
}

void StreamingBarrierHelper::SetCurrentMaxCheckpointIdInQueue(
    const ObjectID &q_id, const uint64_t checkpoint_id) {
  current_max_checkpoint_id_map_[q_id] = checkpoint_id;
}
}  // namespace streaming
}  // namespace ray
