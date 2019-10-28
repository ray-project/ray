#ifndef RAY_STREAMING_CHANNEL_H
#define RAY_STREAMING_CHANNEL_H

#include "streaming.h"
#include "streaming_config.h"
#include "streaming_ring_buffer.h"

namespace ray {
namespace streaming {

class BufferPool;

struct StreamingMessageList {
  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_size = 0;
};

struct StreamingQueueInfo {
  uint64_t first_seq_id = 0;
  uint64_t last_seq_id = 0;
  uint64_t target_seq_id = 0;
  uint64_t consumed_seq_id = 0;
  uint64_t unconsumed_bytes = 0;
};

struct ProducerChannelInfo {
  ObjectID channel_id;
  StreamingRingBufferPtr writer_ring_buffer;
  std::shared_ptr<BufferPool> buffer_pool;
  uint64_t current_message_id;
  uint64_t current_seq_id;
  uint64_t message_last_commit_id;
  uint64_t queue_size;
  StreamingQueueCreationType queue_creation_type;
  StreamingQueueInfo queue_info;

  int64_t message_pass_by_ts;
  uint64_t warning_mark;

  // For Event-Driven
  uint64_t sent_empty_cnt = 0;
  uint64_t flow_control_cnt = 0;
  uint64_t user_event_cnt = 0;
  uint64_t rb_full_cnt = 0;
  uint64_t queue_full_cnt = 0;
  uint64_t in_event_queue_cnt = 0;
  bool in_event_queue_ = false;
  bool flow_control_ = false;
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
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_CHANNEL_H
