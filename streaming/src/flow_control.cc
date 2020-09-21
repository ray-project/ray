#include "flow_control.h"

namespace ray {
namespace streaming {

UnconsumedSeqFlowControl::UnconsumedSeqFlowControl(
    std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map,
    uint32_t step)
    : channel_map_(channel_map), consumed_step_(step){};

bool UnconsumedSeqFlowControl::ShouldFlowControl(ProducerChannelInfo &channel_info) {
  auto &queue_info = channel_info.queue_info;
  if (queue_info.target_message_id <= channel_info.current_message_id) {
    channel_map_[channel_info.channel_id]->RefreshChannelInfo();
    // Target seq id is maximum upper limit in current condition.
    channel_info.queue_info.target_message_id =
        channel_info.queue_info.consumed_message_id + consumed_step_;
    STREAMING_LOG(DEBUG)
        << "Flow control stop writing to downstream, current message id => "
        << channel_info.current_message_id << ", target message id => "
        << queue_info.target_message_id << ", consumed_id => "
        << queue_info.consumed_message_id << ", q id => " << channel_info.channel_id
        << ". if this log keeps printing, it means something wrong "
           "with queue's info API, or downstream node is not "
           "consuming data.";
    // Double check after refreshing if target seq id is changed.
    if (queue_info.target_message_id <= channel_info.current_message_id) {
      return true;
    }
  }
  return false;
}
}  // namespace streaming

}  // namespace ray
