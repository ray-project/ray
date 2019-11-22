#include "streaming_transfer.h"
#include <unordered_map>
namespace ray {
namespace streaming {

ProducerTransfer::ProducerTransfer(std::shared_ptr<Config> &transfer_config)
    : transfer_config_(transfer_config) {}

ConsumerTransfer::ConsumerTransfer(std::shared_ptr<Config> &transfer_config)
    : transfer_config_(transfer_config) {}

StreamingQueueProducer::StreamingQueueProducer(std::shared_ptr<Config> &transfer_config)
    : ProducerTransfer(transfer_config) {
  STREAMING_LOG(INFO) << "Producer Init";

  CoreWorker *core_worker = reinterpret_cast<CoreWorker *>(boost::any_cast<uint64_t>(
      transfer_config_->Get(ConfigEnum::CORE_WORKER, (uint64_t)0)));
  RayFunction async_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::ASYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));
  RayFunction sync_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::SYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));

  queue_writer_ = CreateQueueWriter(
      boost::any_cast<JobID>(transfer_config_->Get(ConfigEnum::CURRENT_DRIVER_ID)),
      boost::any_cast<std::vector<ObjectID>>(
          transfer_config_->Get(ConfigEnum::QUEUE_ID_VECTOR)),
      core_worker, async_func, sync_func);
  STREAMING_CHECK(queue_writer_ != nullptr) << "Create queue writer failed.";
}

StreamingQueueProducer::~StreamingQueueProducer() {
  STREAMING_LOG(INFO) << "Producer Destory";
}

StreamingStatus StreamingQueueProducer::CreateTransferChannel(
    ProducerChannelInfo &channel_info) {
  CreateQueue(channel_info);

  // last commit seq id
  uint64_t queue_last_seq_id = 0;
  uint64_t last_message_id_in_queue = 0;

  last_message_id_in_queue =
      FetchLastMessageIdFromQueue(channel_info.channel_id, queue_last_seq_id);

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

StreamingStatus StreamingQueueProducer::CreateQueue(ProducerChannelInfo &channel_info) {
  auto &channel_id = channel_info.channel_id;
  queue_writer_->CreateQueue(channel_id, channel_info.queue_size,
                                      channel_info.actor_id);

  STREAMING_LOG(INFO) << "q id => " << channel_id << ", queue size => "
                      << channel_info.queue_size;

  return StreamingStatus::OK;
}

uint64_t StreamingQueueProducer::FetchLastMessageIdFromQueue(
    const ObjectID &queue_id, uint64_t &last_queue_seq_id) {
  last_queue_seq_id = 0;
  return 0;
}

StreamingStatus StreamingQueueProducer::DestroyTransferChannel(
    ProducerChannelInfo &channel_info) {
  RAY_IGNORE_EXPR(queue_writer_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ClearTransferCheckpoint(
    ProducerChannelInfo &channel_info, uint64_t checkpoint_id,
    uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::NotifyChannelConsumed(
    ProducerChannelInfo &channel_info, uint64_t channel_offset) {
  Status st =
      queue_writer_->SetQueueEvictionLimit(channel_info.channel_id, channel_offset);
  STREAMING_CHECK(st.code() == StatusCode::OK)
      << " exception in clear barrier in writerwith client returned => " << st.message();
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(
    ProducerChannelInfo &channel_info, uint8_t *data, uint32_t data_size) {
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

StreamingStatus StreamingQueueProducer::WaitChannelsReady(
    std::vector<ObjectID> &channels, uint32_t timeout,
    std::vector<ObjectID> &abnormal_channels) {
  queue_writer_->WaitQueuesInCluster(channels, timeout, abnormal_channels);
  if (abnormal_channels.size()) {
    return StreamingStatus::WaitQueueTimeOut;
  }
  return StreamingStatus::OK;
}

StreamingQueueConsumer::StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config)
    : ConsumerTransfer(transfer_config) {
  STREAMING_LOG(INFO) << "Consumer Init";

  CoreWorker *core_worker = reinterpret_cast<CoreWorker *>(boost::any_cast<uint64_t>(
      transfer_config_->Get(ConfigEnum::CORE_WORKER, (uint64_t)0)));
  RayFunction async_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::ASYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));
  RayFunction sync_func = boost::any_cast<RayFunction>(transfer_config_->Get(
      ConfigEnum::SYNC_FUNCTION, RayFunction{ray::Language::JAVA, {}}));

  queue_reader_ = CreateQueueReader(
      boost::any_cast<JobID>(transfer_config_->Get(ConfigEnum::CURRENT_DRIVER_ID)),
      boost::any_cast<std::vector<ObjectID>>(
          transfer_config_->Get(ConfigEnum::QUEUE_ID_VECTOR)),
      core_worker, async_func, sync_func);
  STREAMING_CHECK(queue_reader_ != nullptr) << "Create queue reader failed.";
}

StreamingQueueConsumer::~StreamingQueueConsumer() {
  STREAMING_LOG(INFO) << "Consumer Destroy";
}

StreamingStatus StreamingQueueConsumer::CreateTransferChannel(
    ConsumerChannelInfo &channel_info) {
  // subscribe next seq id from checkpoint id
  // pull remote queue to local store if scheduler connection is set
  bool success =
      queue_reader_->GetQueue(channel_info.channel_id, -1,
                              channel_info.current_seq_id + 1, channel_info.actor_id);
  if (!success) {
    return StreamingStatus::InitQueueFailed;
  }
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::DestroyTransferChannel(
    ConsumerChannelInfo &channel_info) {
  RAY_IGNORE_EXPR(queue_reader_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ClearTransferCheckpoint(
    ConsumerChannelInfo &channel_info, uint64_t checkpoint_id,
    uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(
    ConsumerChannelInfo &channel_info, uint64_t &offset_id, uint8_t *&data,
    uint32_t &data_size, uint32_t timeout) {
  auto st = queue_reader_->GetQueueItem(channel_info.channel_id, data, data_size,
                                        offset_id, timeout);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::NotifyChannelConsumed(
    ConsumerChannelInfo &channel_info, uint64_t offset_id) {
  queue_reader_->NotifyConsumedItem(channel_info.channel_id, offset_id);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::WaitChannelsReady(
    std::vector<ObjectID> &channels, uint32_t timeout,
    std::vector<ObjectID> &abnormal_channels) {
  queue_reader_->WaitQueuesInCluster(channels, timeout, abnormal_channels);
  if (abnormal_channels.size()) {
    return StreamingStatus::WaitQueueTimeOut;
  }
  return StreamingStatus::OK;
}

// For mock queue transfer
struct MockQueueItem {
  uint64_t seq_id;
  uint32_t data_size;
  std::shared_ptr<uint8_t> data;
};

struct MockQueue {
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBufferImpl<MockQueueItem>>>
      message_buffer_;
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBufferImpl<MockQueueItem>>>
      consumed_buffer_;
};
static MockQueue mock_queue;

StreamingStatus MockProducer::CreateTransferChannel(ProducerChannelInfo &channel_info) {
  mock_queue.message_buffer_[channel_info.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(500);
  mock_queue.consumed_buffer_[channel_info.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(500);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::DestroyTransferChannel(ProducerChannelInfo &channel_info) {
  mock_queue.message_buffer_.erase(channel_info.channel_id);
  mock_queue.consumed_buffer_.erase(channel_info.channel_id);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::ProduceItemToChannel(ProducerChannelInfo &channel_info,
                                                   uint8_t *data, uint32_t data_size) {
  auto &ring_buffer = mock_queue.message_buffer_[channel_info.channel_id];
  if (ring_buffer->Full()) {
    return StreamingStatus::OutOfMemory;
  }
  MockQueueItem item;
  item.seq_id = channel_info.current_seq_id + 1;
  item.data.reset(new uint8_t[data_size]);
  item.data_size = data_size;
  std::memcpy(item.data.get(), data, data_size);
  ring_buffer->Push(item);
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                                     uint64_t &offset_id, uint8_t *&data,
                                                     uint32_t &data_size,
                                                     uint32_t timeout) {
  auto &channel_id = channel_info.channel_id;
  if (mock_queue.message_buffer_.find(channel_id) == mock_queue.message_buffer_.end()) {
    return StreamingStatus::NoSuchItem;
  }

  if (mock_queue.message_buffer_[channel_id]->Empty()) {
    return StreamingStatus::NoSuchItem;
  }
  MockQueueItem item = mock_queue.message_buffer_[channel_id]->Front();
  mock_queue.message_buffer_[channel_id]->Pop();
  mock_queue.consumed_buffer_[channel_id]->Push(item);
  offset_id = item.seq_id;
  data = item.data.get();
  data_size = item.data_size;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::NotifyChannelConsumed(ConsumerChannelInfo &channel_info,
                                                    uint64_t offset_id) {
  auto &channel_id = channel_info.channel_id;
  auto &ring_buffer = mock_queue.consumed_buffer_[channel_id];
  while (!ring_buffer->Empty() && ring_buffer->Front().seq_id <= offset_id) {
    ring_buffer->Pop();
  }
  return StreamingStatus::OK;
}

}  // namespace streaming
}  // namespace ray
