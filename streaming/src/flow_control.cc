#include "flow_control.h"

namespace ray {
namespace streaming {

UnconsumedSeqFlowControl::UnconsumedSeqFlowControl(
    std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map,
    uint32_t step, uint64_t bundle_step = 0xfff)
    : channel_map_(channel_map), message_consumed_step_(step){};

bool UnconsumedSeqFlowControl::ShouldFlowControl(ProducerChannelInfo &channel_info) {
  auto &queue_info = channel_info.queue_info;
  // Data messages should be under flow control if target message id is less then lastest
  // sent message id, and empty message can not be dumped without limitation. So we keep
  // available bucket size of unconsumed message in [0, message consumed step] and
  // unconsumed empty message size in [0, bundle consumed step].
  if (queue_info.target_message_id <= channel_info.current_message_id ||
      chnanel_info.sent_empty_cnt > 0) {
    channel_map_[channel_info.channel_id]->RefreshChannelInfo();
    // Target seq id is maximum upper limit in current condition.
    channel_info.queue_info.target_message_id =
        channel_info.queue_info.consumed_message_id + message_consumed_step_;
    bool message_in_buffer =
        channel_info.current_message_id > channel_info.message_last_commit_id;
    STREAMING_LOG(DEBUG)
            << "Flow control stop writing to downstream, current message id => "
            << channel_info.current_message_id << ", target message id => "
            << queue_info.target_message_id << ", consumed_id => "
            << queue_info.consumed_message_id << " consumed bundle id "
            << queue_info.consumed_bundle_id < < < <
        ", q id => " << channel_info.channel_id << ", has message in buffer "
                     << message_in_buffer
                     << ". if this log keeps printing, it means something wrong "
                        "with queue's info API, or downstream node is not "
                        "consuming data.";
    // Double check whether flow control is valid.
    // A channel will not produce new item if
    // 1. Some messages from upstream to downstream are unconsumed if target message id
    //    is less than last commit message id.
    // 2. All valid messages are consumed, but there are still a lot of empty messages in
    //    downstream waiting list.
    // Besides, we assume queue consumed bundle id will be -1 if no bundle have been
    // consumed by downstream or no notification message received from downstream.
    // In failover case, queue_info.consumed_bundle_id will be updated in old value that
    // may greater than channel_info.current_bundle_id.

    if (queue_info.target_message_id <= channel_info.current_message_id ||
        (!message_in_buffer &&
         channel_info.current_bundle_id > queue_info.consumed_bundle_id &&
         channel_info.current_bundle_id - queue_info.consumed_bundle_id >
             bundle_consumed_step_)) {
      return true;
    }
  }
  return false;
}
}  // namespace streaming

}  // namespace ray
