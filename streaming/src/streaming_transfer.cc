#include "streaming_transfer.h"
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

StreamingQueueProducer::~StreamingQueueProducer() { STREAMING_LOG(INFO) << "Producer Destory"; }

StreamingStatus StreamingQueueProducer::CreateTransferChannel(ProducerChannelInfo &channel_info) {
  auto status = CreateQueue(channel_info);
  if (StreamingStatus::OK != status) {
    // failed queues should be cleaned up in case of old data exists in downstream.
    queue_writer_->CleanupSubscription(channel_info.channel_id);
    return status;
  }
  // last commit seq id
  uint64_t queue_last_seq_id = 0;
  uint64_t last_message_id_in_queue = 0;

    last_message_id_in_queue = FetchLastMessageIdFromQueueForStreamingQueue(
        channel_info.channel_id, queue_last_seq_id);

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
  return status;
}

StreamingStatus StreamingQueueProducer::CreateQueue(ProducerChannelInfo &channel_info) {
  auto &channel_id = channel_info.channel_id;
  bool is_queue_found = queue_writer_->IsQueueFoundInLocal(channel_id);
  STREAMING_LOG(INFO) << "Queue [" << channel_id
                      << "], queue exists: " << is_queue_found;

  ray::Status status;

    // recreate & clear the queue
    queue_writer_->CleanupSubscription(channel_id);
    status = queue_writer_->CreateQueue(channel_id, channel_info.queue_size,
                                        channel_info.actor_id, false, true);

  if (status.code() != StatusCode::OK) {
      STREAMING_LOG(ERROR) << "Create queue [" << channel_id
                           << "] failed of msg: " << status.message();
      RAY_CHECK_OK(status);
  }

  STREAMING_LOG(INFO) << "q id => " << channel_id << ", queue size => "
                      << channel_info.queue_size;

  return StreamingStatus::OK;
}

uint64_t StreamingQueueProducer::FetchLastMessageIdFromQueue(const ObjectID &queue_id,
                                                     uint64_t &last_queue_seq_id) {
  uint32_t data_size;
  std::shared_ptr<uint8_t> data = nullptr;
  queue_writer_->GetLastQueueItem(queue_id, data, data_size, last_queue_seq_id);
  // Queue is empty, just return
  if (data == nullptr) {
    STREAMING_LOG(WARNING) << "Last item of queue [" << queue_id << "] is not found.";
    return 0;
  }
  STREAMING_LOG(INFO) << "Get last queue item, data size: " << data_size
                      << ", queue last seq id: " << last_queue_seq_id;
  auto message_bundle = StreamingMessageBundleMeta::FromBytes(data.get());
  return message_bundle->GetLastMessageId();
}

uint64_t StreamingQueueProducer::FetchLastMessageIdFromQueueForStreamingQueue(
    const ObjectID &queue_id, uint64_t &last_queue_seq_id) {
    last_queue_seq_id = 0;
    return 0;
}

StreamingStatus StreamingQueueProducer::DestroyTransferChannel(
    ProducerChannelInfo &channel_info) {
  RAY_IGNORE_EXPR(queue_writer_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ClearTransferCheckpoint(ProducerChannelInfo &channel_info,
                                                        uint64_t checkpoint_id,
                                                        uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::RefreshChannelInfo(ProducerChannelInfo &channel_info) {
  uint64_t min_consumed_id = 0;
  queue_writer_->GetMinConsumedSeqID(channel_info.channel_id, min_consumed_id);
  if (min_consumed_id != static_cast<uint64_t>(-1)) {
    channel_info.queue_info.consumed_seq_id = min_consumed_id;
  }
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::NotfiyChannelConsumed(ProducerChannelInfo &channel_info,
                                                      uint64_t channel_offset) {
  Status st =
      queue_writer_->SetQueueEvictionLimit(channel_info.channel_id, channel_offset);
  STREAMING_CHECK(st.code() == StatusCode::OK)
      << " exception in clear barrier in writerwith client returned => "
      << st.message();
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(ProducerChannelInfo &channel_info,
                                                     uint8_t *data, uint32_t data_size) {
  Status status = queue_writer_->PushQueueItem(channel_info.channel_id,
                                               channel_info.current_seq_id + 1, data,
                                               data_size, current_sys_time_ms());

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

StreamingQueueConsumer::~StreamingQueueConsumer() { STREAMING_LOG(INFO) << "Consumer Destroy"; }

StreamingStatus StreamingQueueConsumer::CreateTransferChannel(ConsumerChannelInfo &channel_info) {
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

StreamingStatus StreamingQueueConsumer::ClearTransferCheckpoint(ConsumerChannelInfo &channel_info,
                                                        uint64_t checkpoint_id,
                                                        uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::RefreshChannelInfo(ConsumerChannelInfo &channel_info) {
  auto &queue_info = channel_info.queue_info;
  queue_reader_->GetLastSeqID(channel_info.channel_id, queue_info.last_seq_id);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                                       uint64_t &offset_id,
                                                       uint8_t *&data,
                                                       uint32_t &data_size,
                                                       uint32_t timeout) {
  auto st = queue_reader_->GetQueueItem(channel_info.channel_id, data, data_size,
                                        offset_id, timeout);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::NotfiyChannelConsumed(ConsumerChannelInfo &channel_info,
                                                      uint64_t offset_id) {
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

}  // namespace streaming
}  // namespace ray
