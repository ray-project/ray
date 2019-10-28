#include "streaming_transfer.h"
namespace ray {
namespace streaming {

ProducerTransfer::ProducerTransfer(std::shared_ptr<Config> &transfer_config)
    : transfer_config_(transfer_config) {}

ConsumerTransfer::ConsumerTransfer(std::shared_ptr<Config> &transfer_config)
    : transfer_config_(transfer_config) {}

PlasmaProducer::PlasmaProducer(std::shared_ptr<Config> &transfer_config)
    : ProducerTransfer(transfer_config) {
  STREAMING_LOG(INFO) << "Plasma Producer Init";
  plasma_store_socket_path_ =
      transfer_config_->GetString(ConfigEnum::PLASMA_STORE_SOCKET_PATH);
  raylet_client_ = reinterpret_cast<RayletClient *>(
      transfer_config_->GetInt64(ConfigEnum::RAYLET_CLIENT));

  queue_writer_ = CreateQueueWriter(
      plasma_store_socket_path_,
      transfer_config_->GetString(ConfigEnum::RAYLET_SOCKET_PATH),
      boost::any_cast<JobID>(transfer_config_->Get(ConfigEnum::CURRENT_DRIVER_ID)),
      raylet_client_,
      boost::any_cast<std::vector<ObjectID>>(
          transfer_config_->Get(ConfigEnum::QUEUE_ID_VECTOR)));
  STREAMING_CHECK(queue_writer_ != nullptr) << "Create queue writer failed.";
}

PlasmaProducer::~PlasmaProducer() { STREAMING_LOG(INFO) << "Plasma Producer Destory"; }

StreamingStatus PlasmaProducer::CreateTransferChannel(ProducerChannelInfo &channel_info) {
  auto status = CreateQueue(channel_info);
  if (StreamingStatus::OK != status) {
    // failed queues should be cleaned up in case of old data exists in downstream.
    queue_writer_->CleanupSubscription(channel_info.channel_id);
    return status;
  }
  // last commit seq id
  uint64_t queue_last_seq_id = 0;
  uint64_t last_message_id_in_queue =
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
  return status;
}

StreamingStatus PlasmaProducer::CreateQueue(ProducerChannelInfo &channel_info) {
  auto &channel_id = channel_info.channel_id;
  bool is_queue_found = queue_writer_->IsQueueFoundInLocal(channel_id);
  STREAMING_LOG(INFO) << "Queue [" << channel_id
                      << "], queue exists in plasma store: " << is_queue_found;

  ray::Status status;

  // Start seq id only works for createType:Recreate if no object in local store.
  // In other conditions, it's fake parameter(fix me).
  if (StreamingQueueCreationType::RECONSTRUCT == channel_info.queue_creation_type) {
    STREAMING_CHECK(raylet_client_);
    if (queue_writer_->UsePull()) {
      // fetch all obejct from downstream before reconstruction
      queue_writer_->PullQueueToLocal(channel_id);
      // async pull, set timeout to check if queue is done.
      if (!is_queue_found) {
        uint32_t timeout_param =
            (channel_info.queue_size >> 20) > 0 ? (channel_info.queue_size >> 20) : 1;
        is_queue_found = queue_writer_->IsQueueFoundInLocal(
            channel_id,
            transfer_config_->GetInt32(ConfigEnum::RECONSTRUCT_RETRY_TIMES) *
                transfer_config_->GetInt32(ConfigEnum::RECONSTRUCT_TIMEOUT_PER_MB) *
                timeout_param);
        if (!is_queue_found) {
          STREAMING_LOG(INFO) << "Queue " << channel_id
                              << " is not found in local, timeout param: "
                              << timeout_param;
          return StreamingStatus::ReconstructTimeOut;
        }
      }
    }

    status = queue_writer_->CreateQueue(channel_id, channel_info.queue_size, true);
    // notify resubscribe message to reader
    if (!queue_writer_->NotifyResubscribe(channel_id)) {
      return StreamingStatus::ResubscribeFailed;
    }
  } else if (channel_info.queue_creation_type == StreamingQueueCreationType::RECREATE) {
    // recreate queue if local has the same queue object
    status = queue_writer_->CreateQueue(channel_id, channel_info.queue_size, false);
  } else {
    // recreate & clear the queue
    queue_writer_->CleanupSubscription(channel_id);
    status = queue_writer_->CreateQueue(channel_id, channel_info.queue_size, false, true);
  }

  if (status.code() != StatusCode::OK) {
    switch (status.code()) {
    case StatusCode::CapacityError:
      STREAMING_LOG(WARNING) << "Create queue [" << channel_id
                             << "] failed of plasma store full, msg: "
                             << status.message();
      return StreamingStatus::FullPlasmaStore;
    default:
      STREAMING_LOG(ERROR) << "Create queue [" << channel_id
                           << "] failed of msg: " << status.message();
      RAY_CHECK_OK(status);
    }
  }

  STREAMING_LOG(INFO) << "q id => " << channel_id << ", queue size => "
                      << channel_info.queue_size;

  return StreamingStatus::OK;
}

uint64_t PlasmaProducer::FetchLastMessageIdFromQueue(const ObjectID &queue_id,
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

StreamingStatus PlasmaProducer::DestroyTransferChannel(
    ProducerChannelInfo &channel_info) {
  RAY_IGNORE_EXPR(queue_writer_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::ClearTransferCheckpoint(ProducerChannelInfo &channel_info,
                                                        uint64_t checkpoint_id,
                                                        uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::RefreshChannelInfo(ProducerChannelInfo &channel_info) {
  uint64_t min_consumed_id = 0;
  queue_writer_->GetMinConsumedSeqID(channel_info.channel_id, min_consumed_id);
  if (min_consumed_id != static_cast<uint64_t>(-1)) {
    channel_info.queue_info.consumed_seq_id = min_consumed_id;
  }
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::RefreshUnconsumedBytes(
    ProducerChannelInfo &channel_info) {
  uint32_t unconsumed_bytes = 0;
  queue_writer_->GetUnconsumedBytes(channel_info.channel_id, unconsumed_bytes);
  if (unconsumed_bytes != static_cast<uint32_t>(-1)) {
    channel_info.queue_info.unconsumed_bytes = unconsumed_bytes;
  }
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::NotfiyChannelConsumed(ProducerChannelInfo &channel_info,
                                                      uint64_t channel_offset) {
  Status st =
      queue_writer_->SetQueueEvictionLimit(channel_info.channel_id, channel_offset);
  STREAMING_CHECK(st.code() == StatusCode::OK)
      << " exception in clear barrier in writerwith plasma client returned => "
      << st.message();
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::ProduceItemToChannel(ProducerChannelInfo &channel_info,
                                                     uint8_t *data, uint32_t data_size) {
  Status status = queue_writer_->PushQueueItem(channel_info.channel_id,
                                               channel_info.current_seq_id + 1, data,
                                               data_size, current_sys_time_ms());

  if (status.code() != StatusCode::OK) {
    STREAMING_LOG(DEBUG) << channel_info.channel_id << " => Queue is full"
                         << " meesage => " << status.message();

    // Assume that only status OutOfMemory and OK are acceptable.
    // OutOfMemory means plasma-queue is full at that moment.
    STREAMING_CHECK(status.code() == StatusCode::OutOfMemory)
        << "status => " << status.message()
        << ", perhaps data block is so large that it can't be stored in"
        << ", data block size => " << data_size;

    return StreamingStatus::FullChannel;
  }
  return StreamingStatus::OK;
}

StreamingStatus PlasmaProducer::WaitChannelsReady(
    std::vector<ObjectID> &channels, uint32_t timeout,
    std::vector<ObjectID> &abnormal_channels) {
  queue_writer_->WaitQueuesInCluster(channels, timeout, abnormal_channels);
  if (abnormal_channels.size()) {
    return StreamingStatus::WaitQueueTimeOut;
  }
  return StreamingStatus::OK;
}

PlasmaConsumer::PlasmaConsumer(std::shared_ptr<Config> &transfer_config)
    : ConsumerTransfer(transfer_config) {
  STREAMING_LOG(INFO) << "Plasma Consumer Init";
  plasma_store_socket_path_ =
      transfer_config_->GetString(ConfigEnum::PLASMA_STORE_SOCKET_PATH);
  raylet_client_ = reinterpret_cast<RayletClient *>(
      transfer_config_->GetInt64(ConfigEnum::RAYLET_CLIENT));

  queue_reader_ = CreateQueueReader(
      plasma_store_socket_path_,
      transfer_config_->GetString(ConfigEnum::RAYLET_SOCKET_PATH),
      boost::any_cast<JobID>(transfer_config_->Get(ConfigEnum::CURRENT_DRIVER_ID)),
      raylet_client_,
      boost::any_cast<std::vector<ObjectID>>(
          transfer_config_->Get(ConfigEnum::QUEUE_ID_VECTOR)));
  STREAMING_CHECK(queue_reader_ != nullptr) << "Create queue reader failed.";
}

PlasmaConsumer::~PlasmaConsumer() { STREAMING_LOG(INFO) << "Plasma Consumer Destroy"; }

StreamingStatus PlasmaConsumer::CreateTransferChannel(ConsumerChannelInfo &channel_info) {
  // subscribe next seq id from checkpoint id
  // pull remote queue to local store if scheduler connection is set
  bool success = queue_reader_->GetQueue(channel_info.channel_id, -1,
                                         channel_info.current_seq_id + 1);
  if (!success) {
    return StreamingStatus::InitQueueFailed;
  }
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::DestroyTransferChannel(
    ConsumerChannelInfo &channel_info) {
  RAY_IGNORE_EXPR(queue_reader_->DeleteQueue(channel_info.channel_id));
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::ClearTransferCheckpoint(ConsumerChannelInfo &channel_info,
                                                        uint64_t checkpoint_id,
                                                        uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::RefreshChannelInfo(ConsumerChannelInfo &channel_info) {
  auto &queue_info = channel_info.queue_info;
  queue_reader_->GetLastSeqID(channel_info.channel_id, queue_info.last_seq_id);
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                                       uint64_t &offset_id,
                                                       uint8_t *&data,
                                                       uint32_t &data_size,
                                                       uint32_t timeout) {
  auto st = queue_reader_->GetQueueItem(channel_info.channel_id, data, data_size,
                                        offset_id, timeout);
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::NotfiyChannelConsumed(ConsumerChannelInfo &channel_info,
                                                      uint64_t offset_id) {
  queue_reader_->NotifyConsumedItem(channel_info.channel_id, offset_id);
  return StreamingStatus::OK;
}

StreamingStatus PlasmaConsumer::WaitChannelsReady(
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
