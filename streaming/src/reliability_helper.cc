#include "reliability_helper.h"

#include <boost/asio/thread_pool.hpp>
namespace ray {
namespace streaming {

std::shared_ptr<ReliabilityHelper> ReliabilityHelperFactory::CreateReliabilityHelper(
    const StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
    DataWriter *writer, DataReader *reader) {
  if (config.IsExactlyOnce()) {
    return std::make_shared<ExactlyOnceHelper>(config, barrier_helper, writer, reader);
  } else {
    return std::make_shared<AtLeastOnceHelper>(config, barrier_helper, writer, reader);
  }
}

ReliabilityHelper::ReliabilityHelper(const StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : config_(config),
      barrier_helper_(barrier_helper),
      writer_(writer),
      reader_(reader) {}

void ReliabilityHelper::Reload() {}

bool ReliabilityHelper::StoreBundleMeta(ProducerChannelInfo &channel_info,
                                        StreamingMessageBundlePtr &bundle_ptr,
                                        bool is_replay) {
  return false;
}

bool ReliabilityHelper::FilterMessage(ProducerChannelInfo &channel_info,
                                      const uint8_t *data,
                                      StreamingMessageType message_type,
                                      uint64_t *write_message_id) {
  bool is_filtered = false;
  uint64_t &message_id = channel_info.current_message_id;
  uint64_t last_msg_id = channel_info.message_last_commit_id;

  if (StreamingMessageType::Barrier == message_type) {
    is_filtered = message_id < last_msg_id;
  } else {
    message_id++;
    // Message last commit id is the last item in queue or restore from queue.
    // It skip directly since message id is less or equal than current commit id.
    is_filtered = message_id <= last_msg_id && !config_.IsAtLeastOnce();
  }
  *write_message_id = message_id;

  return is_filtered;
}

void ReliabilityHelper::CleanupCheckpoint(ProducerChannelInfo &channel_info,
                                          uint64_t barrier_id) {}

StreamingStatus ReliabilityHelper::InitChannelMerger(uint32_t timeout) {
  return reader_->InitChannelMerger(timeout);
}

StreamingStatus ReliabilityHelper::HandleNoValidItem(ConsumerChannelInfo &channel_info) {
  STREAMING_LOG(DEBUG) << "[Reader] Queue " << channel_info.channel_id
                       << " get item timeout, resend notify "
                       << channel_info.current_message_id;
  reader_->NotifyConsumedItem(channel_info, channel_info.current_message_id);
  return StreamingStatus::OK;
}

AtLeastOnceHelper::AtLeastOnceHelper(const StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : ReliabilityHelper(config, barrier_helper, writer, reader) {}

StreamingStatus AtLeastOnceHelper::InitChannelMerger(uint32_t timeout) {
  // No merge in AT_LEAST_ONCE
  return StreamingStatus::OK;
}

StreamingStatus AtLeastOnceHelper::HandleNoValidItem(ConsumerChannelInfo &channel_info) {
  if (current_sys_time_ms() - channel_info.resend_notify_timer >
      StreamingConfig::RESEND_NOTIFY_MAX_INTERVAL) {
    STREAMING_LOG(INFO) << "[Reader] Queue " << channel_info.channel_id
                        << " get item timeout, resend notify "
                        << channel_info.current_message_id;
    reader_->NotifyConsumedItem(channel_info, channel_info.current_message_id);
    channel_info.resend_notify_timer = current_sys_time_ms();
  }
  return StreamingStatus::Invalid;
}

ExactlyOnceHelper::ExactlyOnceHelper(const StreamingConfig &config,
                                     StreamingBarrierHelper &barrier_helper,
                                     DataWriter *writer, DataReader *reader)
    : ReliabilityHelper(config, barrier_helper, writer, reader) {}

bool ExactlyOnceHelper::FilterMessage(ProducerChannelInfo &channel_info,
                                      const uint8_t *data,
                                      StreamingMessageType message_type,
                                      uint64_t *write_message_id) {
  bool is_filtered = ReliabilityHelper::FilterMessage(channel_info, data, message_type,
                                                      write_message_id);
  if (is_filtered && StreamingMessageType::Barrier == message_type &&
      StreamingRole::SOURCE == config_.GetStreamingRole()) {
    *write_message_id = channel_info.message_last_commit_id;
    // Do not skip source barrier when it's reconstructing from downstream.
    is_filtered = false;
    STREAMING_LOG(INFO) << "append barrier to buffer ring " << *write_message_id
                        << ", last commit id " << channel_info.message_last_commit_id;
  }
  return is_filtered;
}
}  // namespace streaming
}  // namespace ray
