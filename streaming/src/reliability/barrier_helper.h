#pragma once
#include <queue>
#include <unordered_map>

#include "common/status.h"
#include "ray/common/id.h"

namespace ray {
namespace streaming {
class StreamingBarrierHelper {
  using BarrierIdQueue = std::shared_ptr<std::queue<uint64_t>>;

 private:
  // Global barrier map set (global barrier id -> (channel id -> msg id))
  std::unordered_map<uint64_t, std::unordered_map<ObjectID, uint64_t>>
      global_barrier_map_;

  // Message id map to barrier id of each queue(continuous barriers hold same last message
  // id)
  // message id -> (queue id -> list(barrier id)).
  // Thread unsafe to assign value in user's thread but collect it in loopforward thread.
  std::unordered_map<uint64_t, std::unordered_map<ObjectID, BarrierIdQueue>>
      global_reversed_barrier_map_;

  std::unordered_map<uint64_t, uint64_t> barrier_checkpoint_map_;

  std::unordered_map<ObjectID, uint64_t> max_message_id_map_;

  // We assume default max checkpoint is 0.
  std::unordered_map<ObjectID, uint64_t> current_max_checkpoint_id_map_;

  std::mutex message_id_map_barrier_mutex_;

  std::mutex global_barrier_mutex_;

  std::mutex barrier_map_checkpoint_mutex_;

 public:
  StreamingStatus GetMsgIdByBarrierId(const ObjectID &q_id, uint64_t barrier_id,
                                      uint64_t &msg_id);
  void SetMsgIdByBarrierId(const ObjectID &q_id, uint64_t barrier_id, uint64_t seq_id);
  bool Contains(uint64_t barrier_id);
  void ReleaseBarrierMapById(uint64_t barrier_id);
  void ReleaseAllBarrierMap();
  void GetAllBarrier(std::vector<uint64_t> &barrier_id_vec);
  uint32_t GetBarrierMapSize();

  void MapBarrierToCheckpoint(uint64_t barrier_id, uint64_t checkpoint);
  StreamingStatus GetCheckpointIdByBarrierId(uint64_t barrier_id,
                                             uint64_t &checkpoint_id);
  void ReleaseBarrierMapCheckpointByBarrierId(const uint64_t barrier_id);

  StreamingStatus GetBarrierIdByLastMessageId(const ObjectID &q_id, uint64_t message_id,
                                              uint64_t &barrier_id, bool is_pop = false);
  void SetBarrierIdByLastMessageId(const ObjectID &q_id, uint64_t message_id,
                                   uint64_t barrier_id);

  void GetCurrentMaxCheckpointIdInQueue(const ObjectID &q_id,
                                        uint64_t &checkpoint_id) const;

  void SetCurrentMaxCheckpointIdInQueue(const ObjectID &q_id,
                                        const uint64_t checkpoint_id);
};
}  // namespace streaming
}  // namespace ray
