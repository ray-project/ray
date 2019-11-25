#include <unordered_map>
#include "transfer.h"
namespace ray {
namespace streaming {

ProducerChannel::ProducerChannel(std::shared_ptr<Config> &transfer_config,
                                 ProducerChannelInfo &p_channel_info)
    : transfer_config_(transfer_config), channel_info(p_channel_info) {}

ConsumerChannel::ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                                 ConsumerChannelInfo &c_channel_info)
    : transfer_config_(transfer_config), channel_info(c_channel_info) {}

StreamingQueueProducer::StreamingQueueProducer(std::shared_ptr<Config> &transfer_config,
                                               ProducerChannelInfo &p_channel_info)
    : ProducerChannel(transfer_config, p_channel_info) {
  STREAMING_LOG(INFO) << "Producer Init";

  CoreWorker *core_worker = reinterpret_cast<CoreWorker *>(boost::any_cast<uint64_t>(
      transfer_config_->Get(ConfigEnum::CORE_WORKER, (uint64_t)0)));
  RayFunction async_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::ASYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));
  RayFunction sync_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::SYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));

  queue_writer_ =
      std::make_shared<StreamingQueueWriter>(core_worker, async_func, sync_func);
}

StreamingQueueProducer::~StreamingQueueProducer() {
  STREAMING_LOG(INFO) << "Producer Destory";
}

StreamingStatus StreamingQueueProducer::CreateTransferChannel() {
  CreateQueue();

  uint64_t queue_last_seq_id = 0;
  uint64_t last_message_id_in_queue = 0;

  if (!last_message_id_in_queue) {
    if (last_message_id_in_queue < channel_info.current_message_id) {
      STREAMING_LOG(WARNING) << "last message id in queue : " << last_message_id_in_queue
                             << " is less than message checkpoint loaded id : "
                             << channel_info.current_message_id
                             << ", an old queue object " << channel_info.channel_id
                             << " was fond in store";
    }
    last_message_id_in_queue = channel_info.current_message_id;
  }
  if (queue_last_seq_id == static_cast<uint64_t>(-1)) {
    queue_last_seq_id = 0;
  }
  channel_info.current_seq_id = queue_last_seq_id;

  STREAMING_LOG(WARNING) << "existing last message id => " << last_message_id_in_queue
                         << ", message id in channel =>  "
                         << channel_info.current_message_id << ", queue last seq id => "
                         << queue_last_seq_id;

  channel_info.message_last_commit_id = last_message_id_in_queue;
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::CreateQueue() {
  auto &channel_id = channel_info.channel_id;
  queue_writer_->CreateQueue(channel_id, channel_info.queue_size, channel_info.actor_id);

  STREAMING_LOG(INFO) << "q id => " << channel_id << ", queue size => "
                      << channel_info.queue_size;

  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::DestroyTransferChannel() {
  RAY_IGNORE_EXPR(queue_writer_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ClearTransferCheckpoint(
    uint64_t checkpoint_id, uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::NotifyChannelConsumed(uint64_t channel_offset) {
  Status st =
      queue_writer_->SetQueueEvictionLimit(channel_info.channel_id, channel_offset);
  STREAMING_CHECK(st.code() == StatusCode::OK)
      << " exception in clear barrier in writerwith client returned => " << st.message();
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(uint8_t *data,
                                                             uint32_t data_size) {
  Status status = queue_writer_->PushQueueItem(channel_info.channel_id,
                                               channel_info.current_seq_id + 1, data,
                                               data_size, current_time_ms());

  if (status.code() != StatusCode::OK) {
    STREAMING_LOG(DEBUG) << channel_info.channel_id << " => Queue is full"
                         << " meesage => " << status.message();

    // Assume that only status OutOfMemory and OK are acceptable.
    // OutOfMemory means queue is full at that moment.
    STREAMING_CHECK(status.code() == StatusCode::OutOfMemory)
        << "status => " << status.message()
        << ", perhaps data block is so large that it can't be stored in"
        << ", data block size => " << data_size;

    return StreamingStatus::FullChannel;
  }
  return StreamingStatus::OK;
}

StreamingQueueConsumer::StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config,
                                               ConsumerChannelInfo &c_channel_info)
    : ConsumerChannel(transfer_config, c_channel_info) {
  STREAMING_LOG(INFO) << "Consumer Init";

  CoreWorker *core_worker = reinterpret_cast<CoreWorker *>(boost::any_cast<uint64_t>(
      transfer_config_->Get(ConfigEnum::CORE_WORKER, (uint64_t)0)));
  RayFunction async_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::ASYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));
  RayFunction sync_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::SYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));

  queue_reader_ =
      std::make_shared<StreamingQueueReader>(core_worker, async_func, sync_func);
}

StreamingQueueConsumer::~StreamingQueueConsumer() {
  STREAMING_LOG(INFO) << "Consumer Destroy";
}

StreamingStatus StreamingQueueConsumer::CreateTransferChannel() {
  // subscribe next seq id from checkpoint id
  // pull remote queue to local store if scheduler connection is set
  bool success = queue_reader_->GetQueue(
      channel_info.channel_id, channel_info.current_seq_id + 1, channel_info.actor_id);
  if (!success) {
    return StreamingStatus::InitQueueFailed;
  }
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::DestroyTransferChannel() {
  RAY_IGNORE_EXPR(queue_reader_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ClearTransferCheckpoint(
    uint64_t checkpoint_id, uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(uint64_t &offset_id,
                                                               uint8_t *&data,
                                                               uint32_t &data_size,
                                                               uint32_t timeout) {
  auto st = queue_reader_->GetQueueItem(channel_info.channel_id, data, data_size,
                                        offset_id, timeout);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  queue_reader_->NotifyConsumedItem(channel_info.channel_id, offset_id);
  return StreamingStatus::OK;
}

}  // namespace streaming
}  // namespace ray
