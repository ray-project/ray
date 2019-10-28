#include <signal.h>
#include <unistd.h>
#include <chrono>
#include <functional>
#include <list>
#include <numeric>

#include "streaming.h"
#include "streaming_writer.h"

namespace ray {
namespace streaming {

constexpr uint32_t StreamingWriter::kQueueItemMaxBlocks;

void StreamingWriter::WriterLoopForward() {
  STREAMING_CHECK(channel_state_ == StreamingChannelState::Running);
  // Reload operator will be noly executed once if it's exactly same mode
  while (true) {
    int64_t min_passby_message_ts = std::numeric_limits<int64_t>::max();
    uint32_t empty_messge_send_count = 0;

    for (auto &output_queue : output_queue_ids_) {
      if (StreamingChannelState::Running != channel_state_) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (flow_controller_->ShouldFlowControl(channel_info)) {
        continue;
      }
      bool is_push_empty_message = false;
      StreamingStatus write_status =
          WriteChannelProcess(channel_info, &is_push_empty_message);
      int64_t current_ts = current_sys_time_ms();
      if (StreamingStatus::OK == write_status) {
        channel_info.message_pass_by_ts = current_ts;
        if (is_push_empty_message) {
          min_passby_message_ts =
              std::min(channel_info.message_pass_by_ts, min_passby_message_ts);
          empty_messge_send_count++;
          channel_info.sent_empty_cnt++;
        } else {
          channel_info.sent_empty_cnt = 0;
        }
      } else if (StreamingStatus::FullChannel == write_status) {
      } else {
        if (StreamingStatus::EmptyRingBuffer != write_status) {
          STREAMING_LOG(DEBUG) << "write buffer status => "
                               << static_cast<uint32_t>(write_status)
                               << ", is push empty message => " << is_push_empty_message;
        }
      }
    }

    if (empty_messge_send_count == output_queue_ids_.size()) {
      // sleep if empty message was sent in all channel
      uint64_t sleep_time_ = current_sys_time_ms() - min_passby_message_ts;
      // sleep_time can be bigger than time interval because of network jitter
      if (sleep_time_ <= config_.GetStreaming_empty_message_time_interval()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            config_.GetStreaming_empty_message_time_interval() - sleep_time_));
      }
    }
  }
}

StreamingStatus StreamingWriter::WriteChannelProcess(ProducerChannelInfo &channel_info,
                                                     bool *is_empty_message) {
  // no message in buffer, empty message will be sent to downstream queue
  uint64_t buffer_remain = 0;
  StreamingStatus write_queue_flag = WriteBufferToChannel(channel_info, buffer_remain);
  int64_t current_ts = current_sys_time_ms();
  if (write_queue_flag == StreamingStatus::EmptyRingBuffer &&
      current_ts - channel_info.message_pass_by_ts >=
          config_.GetStreaming_empty_message_time_interval()) {
    write_queue_flag = WriteEmptyMessage(channel_info);
    *is_empty_message = true;
    STREAMING_LOG(DEBUG) << "send empty message bundle in q_id =>"
                         << channel_info.channel_id;
  }
  return write_queue_flag;
}

StreamingStatus StreamingWriter::WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                                      uint64_t &buffer_remain) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  if (!IsMessageAvailableInBuffer(channel_info)) {
    return StreamingStatus::EmptyRingBuffer;
  }

  // flush transient buffer to queue first
  if (buffer_ptr->IsTransientAvaliable()) {
    return WriteTransientBufferToChannel(channel_info);
  }

  STREAMING_CHECK(CollectFromRingBuffer(channel_info, buffer_remain))
      << "empty data in ringbuffer, q id => " << channel_info.channel_id;

  return WriteTransientBufferToChannel(channel_info);
}

void StreamingWriter::Run() {
  loop_thread_ =
      std::make_shared<std::thread>(&StreamingWriter::WriterLoopForward, this);
}

uint64_t StreamingWriter::WriteMessageToBufferRing(const ObjectID &q_id, uint8_t *data,
                                                   uint32_t data_size,
                                                   StreamingMessageType message_type) {
  // TODO(lingxuan.zlx): currently, unsafe in multithreads
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  last_write_q_id_ = q_id;
  // Write message id stands for current lastest message id and differs from
  // channel.current_message_id if it's barrier message.
  uint64_t write_message_id;
  if (reliability_helper_->FilterMessage(channel_info, data, message_type,
                                         &write_message_id)) {
    return write_message_id;
  }
  if (StreamingMessageType::Barrier == message_type) {
    StreamingBarrierHeader barrier_header;
    StreamingMessage::GetBarrierIdFromRawData(data, &barrier_header);
    if (barrier_header.IsGlobalBarrier()) {
      barrier_helper_.SetBarrierIdByLastMessageId(q_id, write_message_id,
                                                  barrier_header.barrier_id);
    }
  }
  auto &ring_buffer_ptr = channel_info.writer_ring_buffer;
  uint32_t begin = 0;
  while (ring_buffer_ptr->IsFull() && channel_state_ == StreamingChannelState::Running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.TIME_WAIT_UINT));
    ++begin;
    if (begin == 1) {
      channel_info.rb_full_cnt++;
      STREAMING_LOG(WARNING) << "ringbuffer of qId : " << q_id << " is full, its size => "
                             << ring_buffer_ptr->Size();
    }
  }
  if (channel_state_ != StreamingChannelState::Running) {
    STREAMING_LOG(WARNING) << "stop in write message to ringbuffer";
    return 0;
  }
  std::shared_ptr<uint8_t> msg_data;
  if (message_type == StreamingMessageType::Message) {
    auto status =
        channel_info.buffer_pool->MarkUsed(reinterpret_cast<uint64_t>(data), data_size);
    STREAMING_CHECK(status == StreamingStatus::OK)
        << "mark range [" << reinterpret_cast<void *>(data) << ", "
        << reinterpret_cast<void *>(data + data_size) << "), pool usage "
        << channel_info.buffer_pool->PrintUsage();
    // Since RingBufferImplLockFree doesn't pop items actually (it override item by reader
    // index), we can't rely on ring buffer to pop items in order to release buffer. So we
    // don't place release operations in shared_ptr deleter, we release buffer in
    // CollectFromRingBuffer instead. This is also needed for zero copy bundling messages
    // TODO change StreamingMessage::message_data_ from share_ptr to raw pointer, because
    // buffer is released manually
    msg_data.reset(data, [](uint8_t *p) {});
  } else {
    auto new_buffer = new uint8_t[data_size];
    std::memcpy(new_buffer, data, data_size);
    msg_data.reset(new_buffer, std::default_delete<uint8_t[]>());
  }

  ring_buffer_ptr->Push(std::make_shared<StreamingMessage>(
      msg_data, data_size, write_message_id, message_type));

  return write_message_id;
}

StreamingStatus StreamingWriter::InitChannel(const ObjectID &q_id,
                                             uint64_t channel_message_id,
                                             const std::string &plasma_store_path,
                                             uint64_t queue_size) {
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  channel_info.current_message_id = channel_message_id;
  channel_info.channel_id = q_id;
  barrier_helper_.SetCurrentMaxCheckpointIdInQueue(
      q_id, config_.GetStreaming_rollback_checkpoint_id());
  channel_info.queue_size = queue_size;
  STREAMING_LOG(WARNING) << " Init queue [" << q_id << "], "
                         << " max block size => "
                         << queue_size / config_.GetStreaming_writer_consumed_step();
  // init queue
  channel_info.writer_ring_buffer = std::make_shared<StreamingRingBuffer>(
      config_.GetStreaming_ring_buffer_capacity(), StreamingRingBufferType::SPSC);
  channel_info.buffer_pool =
      std::make_shared<BufferPool>(config_.GetStreaming_buffer_pool_size(),
                                   config_.GetStreaming_buffer_pool_min_buffer_size());
  channel_info.message_pass_by_ts = current_sys_time_ms();
  RETURN_IF_NOT_OK(transfer_->CreateTransferChannel(channel_info));
  return StreamingStatus::OK;
}

StreamingStatus StreamingWriter::Init(const std::vector<ObjectID> &queue_id_vec,
                                      const std::string &plasma_store_path,
                                      const std::vector<uint64_t> &channel_message_id_vec,
                                      const std::vector<uint64_t> &queue_size_vec,
                                      std::vector<ObjectID> &abnormal_queues) {
  STREAMING_CHECK(queue_id_vec.size() && channel_message_id_vec.size());

  if (channel_info_map_.size() != queue_id_vec.size()) {
    for (auto &q_id : queue_id_vec) {
      channel_info_map_[q_id].queue_creation_type = StreamingQueueCreationType::RECREATE;
    }
  }

  std::function<std::string(decltype(channel_info_map_.begin()))> func =
      [](decltype(channel_info_map_.begin()) it) {
        return it->first.Hex() + "->" +
               std::to_string(static_cast<uint64_t>(it->second.queue_creation_type));
      };
  std::string creator_list_str = StreamingUtility::join(
      channel_info_map_.begin(), channel_info_map_.end(), func, "|");
  ray::JobID job_id =
      JobID::FromBinary(StreamingUtility::Hexqid2str(config_.GetStreaming_task_job_id()));

  STREAMING_LOG(INFO) << "Streaming queue creator => " << creator_list_str << ", role => "
                      << streaming::fbs::EnumNameStreamingRole(
                             config_.GetStreaming_role())
                      << ", strategy => "
                      << static_cast<uint32_t>(config_.GetStreaming_strategy_())
                      << ", job name => " << config_.GetStreaming_job_name()
                      << ", log level => " << config_.GetStreaming_log_level()
                      << ", log path => " << config_.GetStreaming_log_path()
                      << ", rollback checkpoint id => "
                      << config_.GetStreaming_rollback_checkpoint_id() << ", job id => "
                      << job_id << ", unified map"
                      << ", flow control => "
                      << streaming::fbs::EnumNameStreamingFlowControlType(
                             config_.GetStreaming_flow_control_type());

  output_queue_ids_ = queue_id_vec;
  std::shared_ptr<Config> transfer_config = std::make_shared<Config>();
  transfer_config->Set(ConfigEnum::PLASMA_STORE_SOCKET_PATH, plasma_store_path);
  transfer_config->Set(ConfigEnum::RAYLET_SOCKET_PATH,
                       config_.GetStreaming_raylet_socket_path());
  transfer_config->Set(ConfigEnum::CURRENT_DRIVER_ID, job_id);
  transfer_config->Set(ConfigEnum::RAYLET_CLIENT,
                       reinterpret_cast<uint64_t>(raylet_client_));
  transfer_config->Set(ConfigEnum::QUEUE_ID_VECTOR, queue_id_vec);
  transfer_config->Set(ConfigEnum::RECONSTRUCT_RETRY_TIMES,
                       config_.GetStreaming_reconstruct_objects_retry_times());
  transfer_config->Set(ConfigEnum::RECONSTRUCT_TIMEOUT_PER_MB,
                       config_.GetStreaming_reconstruct_objects_timeout_per_mb());

  transfer_.reset(new PlasmaProducer(transfer_config));

  std::vector<ObjectID> reconstruct_q_vec;
  std::copy_if(queue_id_vec.begin(), queue_id_vec.end(),
               std::back_inserter(reconstruct_q_vec), [&](const ObjectID &q_id) {
                 return channel_info_map_[q_id].queue_creation_type ==
                        StreamingQueueCreationType::RECONSTRUCT;
               });
  if (reconstruct_q_vec.size() > 0) {
    RETURN_IF_NOT_OK(transfer_->WaitChannelsReady(
        reconstruct_q_vec, config_.GetStreaming_waiting_queue_time_out(),
        abnormal_queues));
  }

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    // init channelIdGenerator or create it
    StreamingStatus status = InitChannel(queue_id_vec[i], channel_message_id_vec[i],
                                         plasma_store_path, queue_size_vec[i]);
    if (status != StreamingStatus::OK) {
      abnormal_queues.insert(abnormal_queues.begin(), queue_id_vec.begin() + i,
                             queue_id_vec.end());
      return status;
    }
  }
  channel_state_ = StreamingChannelState::Running;
  return StreamingStatus::OK;
}

StreamingWriter::~StreamingWriter() {
  // Return if fail to init streaming writer
  if (channel_state_ == StreamingChannelState::Init) {
    return;
  }
  channel_state_ = StreamingChannelState::Interrupted;
  // Shutdown metrics reporter timer
  ShutdownTimer();
  if (loop_thread_->joinable()) {
    STREAMING_LOG(INFO) << "Writer loop thread waiting for join";
    loop_thread_->join();
  }
  // DestroyChannel(output_queue_ids_);
  STREAMING_LOG(INFO) << "Writer client queue disconnect.";
}

bool StreamingWriter::IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info) {
  return channel_info.writer_ring_buffer->IsTransientAvaliable() ||
         !channel_info.writer_ring_buffer->IsEmpty();
}

StreamingStatus StreamingWriter::WriteEmptyMessage(
    ProducerChannelInfo &channel_info) {
  auto &q_id = channel_info.channel_id;
  if (channel_info.message_last_commit_id < channel_info.current_message_id &&
      !meta_ptr) {
    // Abort to send empty message if ring buffer is not empty now.
    STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " abort to send empty, last commit id =>"
                         << channel_info.message_last_commit_id << ", channel max id => "
                         << channel_info.current_message_id;
    return StreamingStatus::SkipSendEmptyMessage;
  }

  // Make an empty bundle, use old ts from reloaded meta if it's not nullptr
  StreamingMessageBundlePtr bundle_ptr = std::make_shared<StreamingMessageBundle>(
      channel_info.current_message_id, current_sys_time_ms());
  auto &q_ringbuffer = channel_info.writer_ring_buffer;
  // Store empty bundle in trasnsitentbuffer if it's exactly same mode
  q_ringbuffer->ReallocTransientBuffer(bundle_ptr->ClassBytesSize());
  bundle_ptr->ToBytes(q_ringbuffer->GetTransientBufferMutable());

  StreamingStatus status = transfer_->ProduceItemToChannel(
      channel_info, const_cast<uint8_t *>(q_ringbuffer->GetTransientBuffer()),
      q_ringbuffer->GetTransientBufferSize());
  STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " send empty message, meta info =>"
                       << bundle_ptr->ToString();

  if (StreamingStatus::FullChannel == status) {
    // Free transient buffer in non-EXACTLY_SAME strategy and we never care
    // about whether empty was sent or not in that condition.
    return status;
  }
  channel_info.current_seq_id++;
  channel_info.message_pass_by_ts = current_sys_time_ms();
  q_ringbuffer->FreeTransientBuffer();
  return StreamingStatus::OK;
}

StreamingStatus StreamingWriter::Init(
    const std::vector<ObjectID> &queue_id_vec, const std::string &plasma_store_path,
    const std::vector<uint64_t> &channel_seq_id_vec,
    const std::vector<uint64_t> &queue_size_vec, std::vector<ObjectID> &abnormal_queues,
    const std::vector<StreamingQueueCreationType> &create_types_vec) {
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    channel_info_map_[queue_id_vec[i]].queue_creation_type = create_types_vec[i];
  }
  return Init(queue_id_vec, plasma_store_path, channel_seq_id_vec, queue_size_vec,
              abnormal_queues);
}

StreamingStatus StreamingWriter::WriteTransientBufferToChannel(
    ProducerChannelInfo &channel_info) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  auto &q_id = channel_info.channel_id;
  StreamingStatus status = transfer_->ProduceItemToChannel(
      channel_info, buffer_ptr->GetTransientBufferMutable(),
      buffer_ptr->GetTransientBufferSize());
  RETURN_IF_NOT_OK(status);
  channel_info.current_seq_id++;
  auto transient_bundle_meta =
      StreamingMessageBundleMeta::FromBytes(buffer_ptr->GetTransientBuffer());
  bool is_barrier_bundle = transient_bundle_meta->IsBarrier();
  if (is_barrier_bundle) {
    StreamingBarrierHeader barrier_header;
    StreamingMessage::GetBarrierIdFromRawData(
        buffer_ptr->GetTransientBuffer() + kMessageBundleHeaderSize + kMessageHeaderSize,
        &barrier_header);
    // Skip partial barrier
    if (barrier_header.IsGlobalBarrier()) {
      uint64_t global_barrier_id = 0;
      StreamingStatus status = barrier_helper_.GetBarrierIdByLastMessageId(
          q_id, transient_bundle_meta->GetLastMessageId(), global_barrier_id, true);
      uint64_t max_queue_seq_id = channel_info.current_seq_id;

      if (StreamingStatus::OK != status) {
        STREAMING_LOG(WARNING)
            << "[Writer] [Barrier] global barrier was removed because of out of memory, "
            << " global barrier id => " << global_barrier_id << " q id => " << q_id
            << ", queue seq id => " << max_queue_seq_id;
      } else {
        barrier_helper_.SetSeqIdByBarrierId(q_id, global_barrier_id, max_queue_seq_id);

        STREAMING_LOG(INFO) << "[Writer] [Barrier] q id => " << q_id
                            << ", global barrier id => " << global_barrier_id
                            << ", queue seq id => " << max_queue_seq_id;
      }
    }
  }
  // force delete if it's barrier bundle
  buffer_ptr->FreeTransientBuffer(is_barrier_bundle);
  channel_info.message_last_commit_id = transient_bundle_meta->GetLastMessageId();
  return StreamingStatus::OK;
}

void ReleaseMessages(const StreamingMessageBundlePtr &bundle,
                     const std::shared_ptr<BufferPool> &buffer_pool) {
  auto msg_list = bundle->GetMessageList();
  STREAMING_LOG(DEBUG) << "message list size " << msg_list.size() << ", "
                       << "bundle size " << bundle->ClassBytesSize();
  std::vector<std::tuple<uint64_t, uint64_t>> release_ranges;
  auto release_start = reinterpret_cast<uint64_t>(msg_list.front()->RawData());
  uint64_t release_end = release_start;
  for (auto &msg_ptr : msg_list) {
    auto start_addr = reinterpret_cast<uint64_t>(msg_ptr->RawData());
    if (start_addr != release_end) {
      release_ranges.emplace_back(release_start, release_end);
      release_start = start_addr;
      release_end = release_start;
    }
    release_end = start_addr + msg_ptr->GetDataSize();
  }
  release_ranges.emplace_back(release_start, release_end);

  for (auto &range : release_ranges) {
    auto start = std::get<0>(range);
    auto end = std::get<1>(range);
    if (release_start == release_end) {
      // Empty message may not correspond a valid address in buffer pool,
      // because the buffer may have been released in previous release if buffer is empty
      // after that release.
      STREAMING_LOG(WARNING) << "Empty message shouldn't happen. "
                             << "message list size " << msg_list.size() << ", "
                             << "bundle size " << bundle->ClassBytesSize();
      continue;
    }
    if (buffer_pool->Release(start, end - start) != StreamingStatus::OK) {
      std::stringstream ss;
      ss << "Release buffer error, current release range: ["
         << reinterpret_cast<void *>(start) << ", " << reinterpret_cast<void *>(end)
         << "). ";
      ss << "Release ranges: [";
      int len = static_cast<int>(release_ranges.size());
      for (int i = 0; i < len; i++) {
        auto r = release_ranges[i];
        if (i != 0) {
          ss << ", ";
        }
        ss << "[" << reinterpret_cast<void *>(std::get<0>(r)) << ", "
           << reinterpret_cast<void *>(std::get<1>(r));
        ss << ")";
      }
      ss << "]. ";
      ss << "pool usage: " << buffer_pool->PrintUsage();
      STREAMING_LOG(FATAL) << ss.str();
    }
  }
}

bool StreamingWriter::CollectFromRingBuffer(
    ProducerChannelInfo &channel_info, uint64_t &buffer_remain) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  auto &q_id = channel_info.channel_id;

  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_buffer_size = 0;
  const uint32_t max_queue_item_size =
      channel_info.queue_size /
      std::max(config_.GetStreaming_writer_consumed_step(), kQueueItemMaxBlocks);
  while (message_list.size() < config_.GetStreaming_ring_buffer_capacity() &&
         !buffer_ptr->IsEmpty()) {
    StreamingMessagePtr &message_ptr = buffer_ptr->Front();
    uint32_t message_total_size = message_ptr->ClassBytesSize();
    if (!message_list.empty() &&
        bundle_buffer_size + message_total_size > max_queue_item_size) {
      STREAMING_LOG(DEBUG) << "message total size " << message_total_size
                           << " max queue item size => " << max_queue_item_size;
      break;
    }
    if (!message_list.empty() && (message_ptr->IsBarrier() ||
        message_list.back()->GetMessageType() != message_ptr->GetMessageType())) {
      break;
    }
    // ClassBytesSize = DataSize + MetaDataSize
    // bundle_buffer_size += message_ptr->GetDataSize();
    bundle_buffer_size += message_total_size;
    message_list.push_back(message_ptr);
    buffer_ptr->Pop();
    buffer_remain = buffer_ptr->Size();
    if (message_ptr->IsBarrier()) {
      break;
    }
  }

  if (bundle_buffer_size >= channel_info.queue_size) {
    STREAMING_LOG(ERROR) << "bundle buffer is too large to store q id => " << q_id
                         << ", bundle size => " << bundle_buffer_size
                         << ", queue size => " << channel_info.queue_size;
  }

  StreamingMessageBundlePtr bundle_ptr;
  bundle_ptr = std::make_shared<StreamingMessageBundle>(
      std::move(message_list), current_sys_time_ms(),
      message_list.back()->GetMessageSeqId(),
      message_list.back()->IsBarrier() ? StreamingMessageBundleType::Barrier
                                        : StreamingMessageBundleType::Bundle,
      bundle_buffer_size);
  buffer_ptr->ReallocTransientBuffer(bundle_ptr->ClassBytesSize());
  bundle_ptr->ToBytes(buffer_ptr->GetTransientBufferMutable());

  if (bundle_ptr->IsBundle()) {
    ReleaseMessages(bundle_ptr, channel_info.buffer_pool);
  }

  STREAMING_CHECK(bundle_ptr->ClassBytesSize() == buffer_ptr->GetTransientBufferSize());
  return true;
}

void StreamingWriter::Stop() {
  channel_state_ = StreamingChannelState::Interrupted;
}

std::shared_ptr<BufferPool> StreamingWriter::GetBufferPool(const ObjectID &qid) {
  return channel_info_map_[qid].buffer_pool;
}

}  // namespace streaming
}  // namespace ray
