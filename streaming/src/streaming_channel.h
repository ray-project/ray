#ifndef RAY_STREAMING_CHANNEL_H
#define RAY_STREAMING_CHANNEL_H

#include "streaming.h"
#include "streaming_config.h"
#include "streaming_ring_buffer.h"

namespace ray {
namespace streaming {

struct StreamingQueueInfo {
  uint64_t first_seq_id = 0;
  uint64_t last_seq_id = 0;
  uint64_t target_seq_id = 0;
};

struct ProducerChannelInfo {
  ObjectID channel_id;
  StreamingRingBufferPtr writer_ring_buffer;
  uint64_t current_message_id;
  uint64_t current_seq_id;
  uint64_t message_last_commit_id;
  StreamingQueueInfo queue_info;
  uint32_t queue_size;
  int64_t message_pass_by_ts;

  // for Direct Call
  uint64_t actor_handle;
};

struct ConsumerChannelInfo {
  ObjectID channel_id;
  uint64_t current_message_id;
  uint64_t current_seq_id;
  uint64_t barrier_id;
  uint64_t partial_barrier_id;

  StreamingQueueInfo queue_info;

  uint64_t last_queue_item_delay;
  uint64_t last_queue_item_latency;
  uint64_t last_queue_target_diff;
  uint64_t get_queue_item_times;

  // for Direct Call
  uint64_t actor_handle;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_CHANNEL_H
