#include <memory>

#include <memory>

#include <signal.h>
#include <unistd.h>
#include <chrono>
#include <functional>
#include <list>
#include <numeric>

#include "data_writer.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

void DataWriter::WriterLoopForward() {
  STREAMING_CHECK(RuntimeStatus::Running == runtime_context_->GetRuntimeStatus());
  while (true) {
    int64_t min_passby_message_ts = std::numeric_limits<int64_t>::max();
    uint32_t empty_messge_send_count = 0;

    for (auto &output_queue : output_queue_ids_) {
      if (RuntimeStatus::Running != runtime_context_->GetRuntimeStatus()) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      bool is_push_empty_message = false;
      StreamingStatus write_status =
          WriteChannelProcess(channel_info, &is_push_empty_message);
      int64_t current_ts = current_time_ms();
      if (StreamingStatus::OK == write_status) {
        channel_info.message_pass_by_ts = current_ts;
        if (is_push_empty_message) {
          min_passby_message_ts =
              std::min(channel_info.message_pass_by_ts, min_passby_message_ts);
          empty_messge_send_count++;
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
      // Sleep if empty message was sent in all channel.
      uint64_t sleep_time_ = current_time_ms() - min_passby_message_ts;
      // Sleep_time can be bigger than time interval because of network jitter.
      if (sleep_time_ <= runtime_context_->GetConfig().GetEmptyMessageTimeInterval()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            runtime_context_->GetConfig().GetEmptyMessageTimeInterval() - sleep_time_));
      }
    }
  }
}

StreamingStatus DataWriter::WriteChannelProcess(ProducerChannelInfo &channel_info,
                                                bool *is_empty_message) {
  // No message in buffer, empty message will be sent to downstream queue.
  uint64_t buffer_remain = 0;
  StreamingStatus write_queue_flag = WriteBufferToChannel(channel_info, buffer_remain);
  int64_t current_ts = current_time_ms();
  if (write_queue_flag == StreamingStatus::EmptyRingBuffer &&
      current_ts - channel_info.message_pass_by_ts >=
          runtime_context_->GetConfig().GetEmptyMessageTimeInterval()) {
    write_queue_flag = WriteEmptyMessage(channel_info);
    *is_empty_message = true;
    STREAMING_LOG(DEBUG) << "send empty message bundle in q_id =>"
                         << channel_info.channel_id;
  }
  return write_queue_flag;
}

StreamingStatus DataWriter::WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                                 uint64_t &buffer_remain) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  if (!IsMessageAvailableInBuffer(channel_info)) {
    return StreamingStatus::EmptyRingBuffer;
  }

  // Flush transient buffer to queue first.
  if (buffer_ptr->IsTransientAvaliable()) {
    return WriteTransientBufferToChannel(channel_info);
  }

  STREAMING_CHECK(CollectFromRingBuffer(channel_info, buffer_remain))
      << "empty data in ringbuffer, q id => " << channel_info.channel_id;

  return WriteTransientBufferToChannel(channel_info);
}

void DataWriter::Run() {
  STREAMING_LOG(INFO) << "WriterLoopForward start";
  loop_thread_ = std::make_shared<std::thread>(&DataWriter::WriterLoopForward, this);
}

/// Since every memory ring buffer's size is limited, when the writing buffer is
/// full, the user thread will be blocked, which will cause backpressure
/// naturally.
uint64_t DataWriter::WriteMessageToBufferRing(const ObjectID &q_id, uint8_t *data,
                                              uint32_t data_size,
                                              StreamingMessageType message_type) {
  STREAMING_LOG(DEBUG) << "WriteMessageToBufferRing q_id: " << q_id
                       << " data_size: " << data_size;
  // TODO(lingxuan.zlx): currently, unsafe in multithreads
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  // Write message id stands for current lastest message id and differs from
  // channel.current_message_id if it's barrier message.
  uint64_t &write_message_id = channel_info.current_message_id;
  write_message_id++;
  auto &ring_buffer_ptr = channel_info.writer_ring_buffer;
  while (ring_buffer_ptr->IsFull() &&
         runtime_context_->GetRuntimeStatus() == RuntimeStatus::Running) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(StreamingConfig::TIME_WAIT_UINT));
  }
  if (runtime_context_->GetRuntimeStatus() != RuntimeStatus::Running) {
    STREAMING_LOG(WARNING) << "stop in write message to ringbuffer";
    return 0;
  }
  ring_buffer_ptr->Push(std::make_shared<StreamingMessage>(
      data, data_size, write_message_id, message_type));

  return write_message_id;
}

StreamingStatus DataWriter::InitChannel(const ObjectID &q_id, const ActorID &actor_id,
                                        uint64_t channel_message_id,
                                        uint64_t queue_size) {
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  channel_info.current_message_id = channel_message_id;
  channel_info.channel_id = q_id;
  channel_info.actor_id = actor_id;
  channel_info.queue_size = queue_size;
  STREAMING_LOG(WARNING) << " Init queue [" << q_id << "]";
  channel_info.writer_ring_buffer = std::make_shared<StreamingRingBuffer>(
      runtime_context_->GetConfig().GetRingBufferCapacity(),
      StreamingRingBufferType::SPSC);
  channel_info.message_pass_by_ts = current_time_ms();
  std::shared_ptr<ProducerChannel> channel;

  if (runtime_context_->IsMockTest()) {
    channel = std::make_shared<MockProducer>(transfer_config_, channel_info);
  } else {
    channel = std::make_shared<StreamingQueueProducer>(transfer_config_, channel_info);
  }

  channel_map_.emplace(q_id, channel);
  RETURN_IF_NOT_OK(channel->CreateTransferChannel())
  return StreamingStatus::OK;
}

StreamingStatus DataWriter::Init(const std::vector<ObjectID> &queue_id_vec,
                                 const std::vector<ActorID> &actor_ids,
                                 const std::vector<uint64_t> &channel_message_id_vec,
                                 const std::vector<uint64_t> &queue_size_vec) {
  STREAMING_CHECK(!queue_id_vec.empty() && !channel_message_id_vec.empty());

  ray::JobID job_id =
      JobID::FromBinary(Util::Hexqid2str(runtime_context_->GetConfig().GetTaskJobId()));

  STREAMING_LOG(INFO) << "Job name => " << runtime_context_->GetConfig().GetJobName()
                      << ", job id => " << job_id;

  output_queue_ids_ = queue_id_vec;
  transfer_config_->Set(ConfigEnum::QUEUE_ID_VECTOR, queue_id_vec);

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    StreamingStatus status = InitChannel(queue_id_vec[i], actor_ids[i],
                                         channel_message_id_vec[i], queue_size_vec[i]);
    if (status != StreamingStatus::OK) {
      return status;
    }
  }
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Running);
  return StreamingStatus::OK;
}

DataWriter::DataWriter(std::shared_ptr<RuntimeContext> &runtime_context)
    : transfer_config_(new Config()), runtime_context_(runtime_context) {}

DataWriter::~DataWriter() {
  // Return if fail to init streaming writer
  if (runtime_context_->GetRuntimeStatus() == RuntimeStatus::Init) {
    return;
  }
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Interrupted);
  if (loop_thread_->joinable()) {
    STREAMING_LOG(INFO) << "Writer loop thread waiting for join";
    loop_thread_->join();
  }
  STREAMING_LOG(INFO) << "Writer client queue disconnect.";
}

bool DataWriter::IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info) {
  return channel_info.writer_ring_buffer->IsTransientAvaliable() ||
         !channel_info.writer_ring_buffer->IsEmpty();
}

StreamingStatus DataWriter::WriteEmptyMessage(ProducerChannelInfo &channel_info) {
  auto &q_id = channel_info.channel_id;
  if (channel_info.message_last_commit_id < channel_info.current_message_id) {
    // Abort to send empty message if ring buffer is not empty now.
    STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " abort to send empty, last commit id =>"
                         << channel_info.message_last_commit_id << ", channel max id => "
                         << channel_info.current_message_id;
    return StreamingStatus::SkipSendEmptyMessage;
  }

  // Make an empty bundle, use old ts from reloaded meta if it's not nullptr.
  StreamingMessageBundlePtr bundle_ptr = std::make_shared<StreamingMessageBundle>(
      channel_info.current_message_id, current_time_ms());
  auto &q_ringbuffer = channel_info.writer_ring_buffer;
  q_ringbuffer->ReallocTransientBuffer(bundle_ptr->ClassBytesSize());
  bundle_ptr->ToBytes(q_ringbuffer->GetTransientBufferMutable());

  StreamingStatus status = channel_map_[q_id]->ProduceItemToChannel(
      const_cast<uint8_t *>(q_ringbuffer->GetTransientBuffer()),
      q_ringbuffer->GetTransientBufferSize());
  STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " send empty message, meta info =>"
                       << bundle_ptr->ToString();

  q_ringbuffer->FreeTransientBuffer();
  RETURN_IF_NOT_OK(status)
  channel_info.current_seq_id++;
  channel_info.message_pass_by_ts = current_time_ms();
  return StreamingStatus::OK;
}

StreamingStatus DataWriter::WriteTransientBufferToChannel(
    ProducerChannelInfo &channel_info) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  StreamingStatus status = channel_map_[channel_info.channel_id]->ProduceItemToChannel(
      buffer_ptr->GetTransientBufferMutable(), buffer_ptr->GetTransientBufferSize());
  RETURN_IF_NOT_OK(status)
  channel_info.current_seq_id++;
  auto transient_bundle_meta =
      StreamingMessageBundleMeta::FromBytes(buffer_ptr->GetTransientBuffer());
  bool is_barrier_bundle = transient_bundle_meta->IsBarrier();
  // Force delete to avoid super block memory isn't released so long
  // if it's barrier bundle.
  buffer_ptr->FreeTransientBuffer(is_barrier_bundle);
  channel_info.message_last_commit_id = transient_bundle_meta->GetLastMessageId();
  return StreamingStatus::OK;
}

bool DataWriter::CollectFromRingBuffer(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  auto &q_id = channel_info.channel_id;

  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_buffer_size = 0;
  const uint32_t max_queue_item_size = channel_info.queue_size;
  while (message_list.size() < runtime_context_->GetConfig().GetRingBufferCapacity() &&
         !buffer_ptr->IsEmpty()) {
    StreamingMessagePtr &message_ptr = buffer_ptr->Front();
    uint32_t message_total_size = message_ptr->ClassBytesSize();
    if (!message_list.empty() &&
        bundle_buffer_size + message_total_size >= max_queue_item_size) {
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
      std::move(message_list), current_time_ms(), message_list.back()->GetMessageSeqId(),
      StreamingMessageBundleType::Bundle, bundle_buffer_size);
  buffer_ptr->ReallocTransientBuffer(bundle_ptr->ClassBytesSize());
  bundle_ptr->ToBytes(buffer_ptr->GetTransientBufferMutable());

  STREAMING_CHECK(bundle_ptr->ClassBytesSize() == buffer_ptr->GetTransientBufferSize());
  return true;
}

void DataWriter::Stop() {
  for (auto &output_queue : output_queue_ids_) {
    ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
    while (!channel_info.writer_ring_buffer->IsEmpty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Interrupted);
}
}  // namespace streaming
}  // namespace ray
