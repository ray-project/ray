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
        } else {
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
  loop_thread_ = std::make_shared<std::thread>(&StreamingWriter::WriterLoopForward, this);
}

uint64_t StreamingWriter::WriteMessageToBufferRing(const ObjectID &q_id, uint8_t *data,
                                                   uint32_t data_size,
                                                   StreamingMessageType message_type) {
  // TODO(lingxuan.zlx): currently, unsafe in multithreads
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  last_write_q_id_ = q_id;
  // Write message id stands for current lastest message id and differs from
  // channel.current_message_id if it's barrier message.
  uint64_t &write_message_id = channel_info.current_message_id;
  auto &ring_buffer_ptr = channel_info.writer_ring_buffer;
  while (ring_buffer_ptr->IsFull() && channel_state_ == StreamingChannelState::Running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.TIME_WAIT_UINT));
  }
  if (channel_state_ != StreamingChannelState::Running) {
    STREAMING_LOG(WARNING) << "stop in write message to ringbuffer";
    return 0;
  }
  std::shared_ptr<uint8_t> msg_data;
  auto new_buffer = new uint8_t[data_size];
  std::memcpy(new_buffer, data, data_size);
  msg_data.reset(new_buffer);

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
  channel_info.queue_size = queue_size;
  STREAMING_LOG(WARNING) << " Init queue [" << q_id << "]";
  // init queue
  channel_info.writer_ring_buffer = std::make_shared<StreamingRingBuffer>(
      config_.GetStreaming_ring_buffer_capacity(), StreamingRingBufferType::SPSC);
  channel_info.message_pass_by_ts = current_sys_time_ms();
  RETURN_IF_NOT_OK(transfer_->CreateTransferChannel(channel_info));
  return StreamingStatus::OK;
}

StreamingStatus StreamingWriter::Init(const std::vector<ObjectID> &queue_id_vec,
                                      const std::string &plasma_store_path,
                                      const std::vector<uint64_t> &channel_message_id_vec,
                                      const std::vector<uint64_t> &queue_size_vec) {
  STREAMING_CHECK(queue_id_vec.size() && channel_message_id_vec.size());

  ray::JobID job_id =
      JobID::FromBinary(StreamingUtility::Hexqid2str(config_.GetStreaming_task_job_id()));

  STREAMING_LOG(INFO) << "role => "
                      << streaming::fbs::EnumNameStreamingRole(
                             config_.GetStreaming_role())
                      << ", strategy => "
                      << ", job name => " << config_.GetStreaming_job_name()
                      << ", log level => " << config_.GetStreaming_log_level()
                      << ", log path => " << config_.GetStreaming_log_path() << job_id
                      << ", unified map";

  output_queue_ids_ = queue_id_vec;
  std::shared_ptr<Config> transfer_config = std::make_shared<Config>();
  transfer_config->Set(ConfigEnum::PLASMA_STORE_SOCKET_PATH, plasma_store_path);
  transfer_config->Set(ConfigEnum::RAYLET_SOCKET_PATH,
                       config_.GetStreaming_raylet_socket_path());
  transfer_config->Set(ConfigEnum::CURRENT_DRIVER_ID, job_id);
  transfer_config->Set(ConfigEnum::RAYLET_CLIENT,
                       reinterpret_cast<uint64_t>(raylet_client_));
  transfer_config->Set(ConfigEnum::QUEUE_ID_VECTOR, queue_id_vec);

  transfer_.reset(new MockProducer(transfer_config));

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    // init channelIdGenerator or create it
    StreamingStatus status = InitChannel(queue_id_vec[i], channel_message_id_vec[i],
                                         plasma_store_path, queue_size_vec[i]);
    if (status != StreamingStatus::OK) {
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

StreamingStatus StreamingWriter::WriteEmptyMessage(ProducerChannelInfo &channel_info) {
  auto &q_id = channel_info.channel_id;
  if (channel_info.message_last_commit_id < channel_info.current_message_id) {
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

StreamingStatus StreamingWriter::WriteTransientBufferToChannel(
    ProducerChannelInfo &channel_info) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  StreamingStatus status = transfer_->ProduceItemToChannel(
      channel_info, buffer_ptr->GetTransientBufferMutable(),
      buffer_ptr->GetTransientBufferSize());
  RETURN_IF_NOT_OK(status);
  channel_info.current_seq_id++;
  auto transient_bundle_meta =
      StreamingMessageBundleMeta::FromBytes(buffer_ptr->GetTransientBuffer());
  bool is_barrier_bundle = transient_bundle_meta->IsBarrier();
  // force delete if it's barrier bundle
  buffer_ptr->FreeTransientBuffer(is_barrier_bundle);
  channel_info.message_last_commit_id = transient_bundle_meta->GetLastMessageId();
  return StreamingStatus::OK;
}

void ReleaseMessages(const StreamingMessageBundlePtr &bundle) {
  auto msg_list = bundle->GetMessageList();
  STREAMING_LOG(DEBUG) << "message list size " << msg_list.size() << ", "
                       << "bundle size " << bundle->ClassBytesSize();
  msg_list.clear();
}

bool StreamingWriter::CollectFromRingBuffer(ProducerChannelInfo &channel_info,
                                            uint64_t &buffer_remain) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  auto &q_id = channel_info.channel_id;

  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_buffer_size = 0;
  const uint32_t max_queue_item_size = channel_info.queue_size;
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
    if (!message_list.empty() &&
        message_list.back()->GetMessageType() != message_ptr->GetMessageType()) {
      break;
    }
    // ClassBytesSize = DataSize + MetaDataSize
    // bundle_buffer_size += message_ptr->GetDataSize();
    bundle_buffer_size += message_total_size;
    message_list.push_back(message_ptr);
    buffer_ptr->Pop();
    buffer_remain = buffer_ptr->Size();
  }

  if (bundle_buffer_size >= channel_info.queue_size) {
    STREAMING_LOG(ERROR) << "bundle buffer is too large to store q id => " << q_id
                         << ", bundle size => " << bundle_buffer_size
                         << ", queue size => " << channel_info.queue_size;
  }

  StreamingMessageBundlePtr bundle_ptr;
  bundle_ptr = std::make_shared<StreamingMessageBundle>(
      std::move(message_list), current_sys_time_ms(),
      message_list.back()->GetMessageSeqId(), StreamingMessageBundleType::Bundle,
      bundle_buffer_size);
  buffer_ptr->ReallocTransientBuffer(bundle_ptr->ClassBytesSize());
  bundle_ptr->ToBytes(buffer_ptr->GetTransientBufferMutable());
  ReleaseMessages(bundle_ptr);

  STREAMING_CHECK(bundle_ptr->ClassBytesSize() == buffer_ptr->GetTransientBufferSize());
  return true;
}

void StreamingWriter::Stop() { channel_state_ = StreamingChannelState::Interrupted; }

}  // namespace streaming
}  // namespace ray
