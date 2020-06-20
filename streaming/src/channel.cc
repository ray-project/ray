#include "channel.h"
#include <unordered_map>
namespace ray {
namespace streaming {

ProducerChannel::ProducerChannel(std::shared_ptr<Config> &transfer_config,
                                 ProducerChannelInfo &p_channel_info)
    : transfer_config_(transfer_config), channel_info_(p_channel_info) {}

ConsumerChannel::ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                                 ConsumerChannelInfo &c_channel_info)
    : transfer_config_(transfer_config), channel_info_(c_channel_info) {}

StreamingQueueProducer::StreamingQueueProducer(std::shared_ptr<Config> &transfer_config,
                                               ProducerChannelInfo &p_channel_info)
    : ProducerChannel(transfer_config, p_channel_info) {
  STREAMING_LOG(INFO) << "Producer Init";
}

StreamingQueueProducer::~StreamingQueueProducer() {
  STREAMING_LOG(INFO) << "Producer Destory";
}

StreamingStatus StreamingQueueProducer::CreateTransferChannel() {
  CreateQueue();

  uint64_t queue_last_seq_id = 0;
  uint64_t last_message_id_in_queue = 0;

  if (!last_message_id_in_queue) {
    if (last_message_id_in_queue < channel_info_.current_message_id) {
      STREAMING_LOG(WARNING) << "last message id in queue : " << last_message_id_in_queue
                             << " is less than message checkpoint loaded id : "
                             << channel_info_.current_message_id
                             << ", an old queue object " << channel_info_.channel_id
                             << " was fond in store";
    }
    last_message_id_in_queue = channel_info_.current_message_id;
  }
  if (queue_last_seq_id == static_cast<uint64_t>(-1)) {
    queue_last_seq_id = 0;
  }
  channel_info_.current_seq_id = queue_last_seq_id;

  STREAMING_LOG(WARNING) << "existing last message id => " << last_message_id_in_queue
                         << ", message id in channel =>  "
                         << channel_info_.current_message_id << ", queue last seq id => "
                         << queue_last_seq_id;

  channel_info_.message_last_commit_id = last_message_id_in_queue;
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::CreateQueue() {
  STREAMING_LOG(INFO) << "CreateQueue qid: " << channel_info_.channel_id
                      << " data_size: " << channel_info_.queue_size;
  auto upstream_handler = ray::streaming::UpstreamQueueMessageHandler::GetService();
  if (upstream_handler->UpstreamQueueExists(channel_info_.channel_id)) {
    RAY_LOG(INFO) << "StreamingQueueWriter::CreateQueue duplicate!!!";
    return StreamingStatus::OK;
  }

  upstream_handler->SetPeerActorID(
      channel_info_.channel_id, channel_info_.parameter.actor_id,
      *channel_info_.parameter.async_function, *channel_info_.parameter.sync_function);
  queue_ = upstream_handler->CreateUpstreamQueue(channel_info_.channel_id,
                                                 channel_info_.parameter.actor_id,
                                                 channel_info_.queue_size);
  STREAMING_CHECK(queue_ != nullptr);

  std::vector<ObjectID> queue_ids, failed_queues;
  queue_ids.push_back(channel_info_.channel_id);
  upstream_handler->WaitQueues(queue_ids, 10 * 1000, failed_queues);

  STREAMING_LOG(INFO) << "q id => " << channel_info_.channel_id << ", queue size => "
                      << channel_info_.queue_size;

  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::DestroyTransferChannel() {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ClearTransferCheckpoint(
    uint64_t checkpoint_id, uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::RefreshChannelInfo() {
  channel_info_.queue_info.consumed_seq_id = queue_->GetMinConsumedSeqID();
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::NotifyChannelConsumed(uint64_t channel_offset) {
  queue_->SetQueueEvictionLimit(channel_offset);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(uint8_t *data,
                                                             uint32_t data_size) {
  Status status =
      PushQueueItem(channel_info_.current_seq_id + 1, data, data_size, current_time_ms());

  if (status.code() != StatusCode::OK) {
    STREAMING_LOG(DEBUG) << channel_info_.channel_id << " => Queue is full"
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

Status StreamingQueueProducer::PushQueueItem(uint64_t seq_id, uint8_t *data,
                                             uint32_t data_size, uint64_t timestamp) {
  STREAMING_LOG(INFO) << "StreamingQueueProducer::PushQueueItem:"
                      << " qid: " << channel_info_.channel_id << " seq_id: " << seq_id
                      << " data_size: " << data_size;
  Status status = queue_->Push(seq_id, data, data_size, timestamp, false);
  if (status.IsOutOfMemory()) {
    status = queue_->TryEvictItems();
    if (!status.ok()) {
      STREAMING_LOG(INFO) << "Evict fail.";
      return status;
    }

    status = queue_->Push(seq_id, data, data_size, timestamp, false);
  }

  queue_->Send();
  return status;
}

StreamingQueueConsumer::StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config,
                                               ConsumerChannelInfo &c_channel_info)
    : ConsumerChannel(transfer_config, c_channel_info) {
  STREAMING_LOG(INFO) << "Consumer Init";
}

StreamingQueueConsumer::~StreamingQueueConsumer() {
  STREAMING_LOG(INFO) << "Consumer Destroy";
}

StreamingStatus StreamingQueueConsumer::CreateTransferChannel() {
  auto downstream_handler = ray::streaming::DownstreamQueueMessageHandler::GetService();
  STREAMING_LOG(INFO) << "GetQueue qid: " << channel_info_.channel_id
                      << " start_seq_id: " << channel_info_.current_seq_id + 1;
  if (downstream_handler->DownstreamQueueExists(channel_info_.channel_id)) {
    RAY_LOG(INFO) << "StreamingQueueReader::GetQueue duplicate!!!";
    return StreamingStatus::OK;
  }

  downstream_handler->SetPeerActorID(
      channel_info_.channel_id, channel_info_.parameter.actor_id,
      *channel_info_.parameter.async_function, *channel_info_.parameter.sync_function);
  STREAMING_LOG(INFO) << "Create ReaderQueue " << channel_info_.channel_id
                      << " pull from start_seq_id: " << channel_info_.current_seq_id + 1;
  queue_ = downstream_handler->CreateDownstreamQueue(channel_info_.channel_id,
                                                     channel_info_.parameter.actor_id);

  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::DestroyTransferChannel() {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ClearTransferCheckpoint(
    uint64_t checkpoint_id, uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::RefreshChannelInfo() {
  channel_info_.queue_info.last_seq_id = queue_->GetLastRecvSeqId();
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(uint64_t &offset_id,
                                                               uint8_t *&data,
                                                               uint32_t &data_size,
                                                               uint32_t timeout) {
  STREAMING_LOG(INFO) << "GetQueueItem qid: " << channel_info_.channel_id;
  STREAMING_CHECK(queue_ != nullptr);
  QueueItem item = queue_->PopPendingBlockTimeout(timeout * 1000);
  if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
    STREAMING_LOG(INFO) << "GetQueueItem timeout.";
    data = nullptr;
    data_size = 0;
    offset_id = QUEUE_INVALID_SEQ_ID;
    return StreamingStatus::OK;
  }

  data = item.Buffer()->Data();
  offset_id = item.SeqId();
  data_size = item.Buffer()->Size();

  STREAMING_LOG(DEBUG) << "GetQueueItem qid: " << channel_info_.channel_id
                       << " seq_id: " << offset_id << " msg_id: " << item.MaxMsgId()
                       << " data_size: " << data_size;
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  STREAMING_CHECK(queue_ != nullptr);
  queue_->OnConsumed(offset_id);
  return StreamingStatus::OK;
}

// For mock queue transfer
struct MockQueueItem {
  uint64_t seq_id;
  uint32_t data_size;
  std::shared_ptr<uint8_t> data;
};

class MockQueue {
 public:
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBuffer<MockQueueItem>>>
      message_bffer;
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBuffer<MockQueueItem>>>
      consumed_buffer;
  std::unordered_map<ObjectID, StreamingQueueInfo> queue_info_map;
  static std::mutex mutex;
  static MockQueue &GetMockQueue() {
    static MockQueue mock_queue;
    return mock_queue;
  }
};
std::mutex MockQueue::mutex;

StreamingStatus MockProducer::CreateTransferChannel() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  mock_queue.message_bffer[channel_info_.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(10000);
  mock_queue.consumed_buffer[channel_info_.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(10000);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::DestroyTransferChannel() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  mock_queue.message_bffer.erase(channel_info_.channel_id);
  mock_queue.consumed_buffer.erase(channel_info_.channel_id);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::ProduceItemToChannel(uint8_t *data, uint32_t data_size) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &ring_buffer = mock_queue.message_bffer[channel_info_.channel_id];
  if (ring_buffer->Full()) {
    return StreamingStatus::OutOfMemory;
  }
  MockQueueItem item;
  item.seq_id = channel_info_.current_seq_id + 1;
  item.data.reset(new uint8_t[data_size]);
  item.data_size = data_size;
  std::memcpy(item.data.get(), data, data_size);
  ring_buffer->Push(item);
  mock_queue.queue_info_map[channel_info_.channel_id].last_seq_id = item.seq_id;
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::RefreshChannelInfo() {
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  channel_info_.queue_info.consumed_seq_id =
      mock_queue.queue_info_map[channel_info_.channel_id].consumed_seq_id;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                                     uint32_t &data_size,
                                                     uint32_t timeout) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &channel_id = channel_info_.channel_id;
  if (mock_queue.message_bffer.find(channel_id) == mock_queue.message_bffer.end()) {
    return StreamingStatus::NoSuchItem;
  }

  if (mock_queue.message_bffer[channel_id]->Empty()) {
    return StreamingStatus::NoSuchItem;
  }
  MockQueueItem item = mock_queue.message_bffer[channel_id]->Front();
  mock_queue.message_bffer[channel_id]->Pop();
  mock_queue.consumed_buffer[channel_id]->Push(item);
  offset_id = item.seq_id;
  data = item.data.get();
  data_size = item.data_size;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &channel_id = channel_info_.channel_id;
  auto &ring_buffer = mock_queue.consumed_buffer[channel_id];
  while (!ring_buffer->Empty() && ring_buffer->Front().seq_id <= offset_id) {
    ring_buffer->Pop();
  }
  mock_queue.queue_info_map[channel_id].consumed_seq_id = offset_id;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::RefreshChannelInfo() {
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  channel_info_.queue_info.last_seq_id =
      mock_queue.queue_info_map[channel_info_.channel_id].last_seq_id;
  return StreamingStatus::OK;
}

}  // namespace streaming
}  // namespace ray
