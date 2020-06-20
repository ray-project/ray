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
  STREAMING_LOG(INFO) << "Event server start";
  event_service_->Run();
  // Enable empty message timer after writer running.
  empty_message_thread_ =
      std::make_shared<std::thread>(&DataWriter::EmptyMessageTimerCallback, this);
  flow_control_thread_ =
      std::make_shared<std::thread>(&DataWriter::FlowControlTimer, this);
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

  if (ring_buffer_ptr->Size() == 1) {
    if (channel_info.in_event_queue) {
      ++channel_info.in_event_queue_cnt;
      STREAMING_LOG(DEBUG) << "user_event had been in event_queue";
    } else if (!channel_info.flow_control) {
      channel_info.in_event_queue = true;
      Event event{&channel_info, EventType::UserEvent, false};
      event_service_->Push(event);
      ++channel_info.user_event_cnt;
    }
  }

  return write_message_id;
}

StreamingStatus DataWriter::InitChannel(const ObjectID &q_id,
                                        const ChannelCreationParameter &param,
                                        uint64_t channel_message_id,
                                        uint64_t queue_size) {
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  channel_info.current_message_id = channel_message_id;
  channel_info.channel_id = q_id;
  channel_info.parameter = param;
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
                                 const std::vector<ChannelCreationParameter> &init_params,
                                 const std::vector<uint64_t> &channel_message_id_vec,
                                 const std::vector<uint64_t> &queue_size_vec) {
  STREAMING_CHECK(!queue_id_vec.empty() && !channel_message_id_vec.empty());
  STREAMING_LOG(INFO) << "Job name => " << runtime_context_->GetConfig().GetJobName();

  output_queue_ids_ = queue_id_vec;
  transfer_config_->Set(ConfigEnum::QUEUE_ID_VECTOR, queue_id_vec);

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    StreamingStatus status = InitChannel(queue_id_vec[i], init_params[i],
                                         channel_message_id_vec[i], queue_size_vec[i]);
    if (status != StreamingStatus::OK) {
      return status;
    }
  }

  switch (runtime_context_->GetConfig().GetFlowControlType()) {
  case proto::FlowControlType::UnconsumedSeqFlowControl:
    flow_controller_ = std::make_shared<UnconsumedSeqFlowControl>(
        channel_map_, runtime_context_->GetConfig().GetWriterConsumedStep());
    break;
  default:
    flow_controller_ = std::make_shared<NoFlowControl>();
    break;
  }
  // Register empty event and user event to event server.
  event_service_ = std::make_shared<EventService>();
  event_service_->Register(
      EventType::EmptyEvent,
      std::bind(&DataWriter::SendEmptyToChannel, this, std::placeholders::_1));
  event_service_->Register(EventType::UserEvent, std::bind(&DataWriter::WriteAllToChannel,
                                                           this, std::placeholders::_1));
  event_service_->Register(EventType::FlowEvent, std::bind(&DataWriter::WriteAllToChannel,
                                                           this, std::placeholders::_1));

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
  if (event_service_) {
    event_service_->Stop();
    if (empty_message_thread_->joinable()) {
      STREAMING_LOG(INFO) << "Empty message thread waiting for join";
      empty_message_thread_->join();
    }
    if (flow_control_thread_->joinable()) {
      STREAMING_LOG(INFO) << "FlowControl timer thread waiting for join";
      flow_control_thread_->join();
    }
    int user_event_count = 0;
    int empty_event_count = 0;
    int flow_control_event_count = 0;
    int in_event_queue_cnt = 0;
    int queue_full_cnt = 0;
    for (auto &output_queue : output_queue_ids_) {
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      user_event_count += channel_info.user_event_cnt;
      empty_event_count += channel_info.sent_empty_cnt;
      flow_control_event_count += channel_info.flow_control_cnt;
      in_event_queue_cnt += channel_info.in_event_queue_cnt;
      queue_full_cnt += channel_info.queue_full_cnt;
    }
    STREAMING_LOG(WARNING) << "User event nums: " << user_event_count
                           << ", empty event nums: " << empty_event_count
                           << ", flow control event nums: " << flow_control_event_count
                           << ", queue full nums: " << queue_full_cnt
                           << ", in event queue: " << in_event_queue_cnt;
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

bool DataWriter::WriteAllToChannel(ProducerChannelInfo *info) {
  ProducerChannelInfo &channel_info = *info;
  channel_info.in_event_queue = false;
  while (true) {
    if (RuntimeStatus::Running != runtime_context_->GetRuntimeStatus()) {
      return false;
    }
    // Stop to write remained messages to channel if channel has been blocked by
    // flow control.
    if (channel_info.flow_control) {
      break;
    }
    // Check this channel is blocked by flow control or not.
    if (flow_controller_->ShouldFlowControl(channel_info)) {
      channel_info.flow_control = true;
      break;
    }
    uint64_t ring_buffer_remain = channel_info.writer_ring_buffer->Size();
    StreamingStatus write_status = WriteBufferToChannel(channel_info, ring_buffer_remain);
    int64_t current_ts = current_time_ms();
    if (StreamingStatus::OK == write_status) {
      channel_info.message_pass_by_ts = current_ts;
    } else if (StreamingStatus::FullChannel == write_status ||
               StreamingStatus::OutOfMemory == write_status) {
      channel_info.flow_control = true;
      ++channel_info.queue_full_cnt;
      STREAMING_LOG(DEBUG) << "FullChannel after writing to channel, queue_full_cnt:"
                           << channel_info.queue_full_cnt;
      RefreshChannelAndNotifyConsumed(channel_info);
    } else if (StreamingStatus::EmptyRingBuffer != write_status) {
      STREAMING_LOG(INFO) << channel_info.channel_id
                          << ":something wrong when WriteToQueue "
                          << "write buffer status => "
                          << static_cast<uint32_t>(write_status);
      break;
    }
    if (ring_buffer_remain == 0 &&
        !channel_info.writer_ring_buffer->IsTransientAvaliable()) {
      break;
    }
  }
  return true;
}

bool DataWriter::SendEmptyToChannel(ProducerChannelInfo *channel_info) {
  WriteEmptyMessage(*channel_info);
  return true;
}

void DataWriter::EmptyMessageTimerCallback() {
  while (true) {
    if (RuntimeStatus::Running != runtime_context_->GetRuntimeStatus()) {
      return;
    }

    int64_t current_ts = current_time_ms();
    int64_t min_passby_message_ts = current_ts;
    int count = 0;
    for (auto output_queue : output_queue_ids_) {
      if (RuntimeStatus::Running != runtime_context_->GetRuntimeStatus()) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (channel_info.flow_control || channel_info.writer_ring_buffer->Size() ||
          current_ts < channel_info.message_pass_by_ts) {
        continue;
      }
      if (current_ts - channel_info.message_pass_by_ts >=
          runtime_context_->GetConfig().GetEmptyMessageTimeInterval()) {
        Event event{&channel_info, EventType::EmptyEvent, true};
        event_service_->Push(event);
        ++channel_info.sent_empty_cnt;
        ++count;
        continue;
      }
      if (min_passby_message_ts > channel_info.message_pass_by_ts) {
        min_passby_message_ts = channel_info.message_pass_by_ts;
      }
    }
    STREAMING_LOG(DEBUG) << "EmptyThd:produce empty_events:" << count
                         << " eventqueue size:" << event_service_->EventNums()
                         << " next_sleep_time:"
                         << runtime_context_->GetConfig().GetEmptyMessageTimeInterval() -
                                current_ts + min_passby_message_ts;

    for (const auto &output_queue : output_queue_ids_) {
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      STREAMING_LOG(DEBUG) << output_queue << "==ring_buffer size:"
                           << channel_info.writer_ring_buffer->Size()
                           << " transient_buffer size:"
                           << channel_info.writer_ring_buffer->GetTransientBufferSize()
                           << " in_event_queue:" << channel_info.in_event_queue
                           << " flow_control:" << channel_info.flow_control
                           << " user_event_cnt:" << channel_info.user_event_cnt
                           << " flow_control_event:" << channel_info.flow_control_cnt
                           << " empty_event_cnt:" << channel_info.sent_empty_cnt
                           << " rb_full_cnt:" << channel_info.rb_full_cnt
                           << " queue_full_cnt:" << channel_info.queue_full_cnt;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(
        runtime_context_->GetConfig().GetEmptyMessageTimeInterval() - current_ts +
        min_passby_message_ts));
  }
}

void DataWriter::RefreshChannelAndNotifyConsumed(ProducerChannelInfo &channel_info) {
  // Refresh current downstream consumed seq id.
  channel_map_[channel_info.channel_id]->RefreshChannelInfo();
  // Notify the consumed information to local channel.
  NotifyConsumedItem(channel_info, channel_info.queue_info.consumed_seq_id);
}

void DataWriter::NotifyConsumedItem(ProducerChannelInfo &channel_info, uint32_t offset) {
  if (offset > channel_info.current_seq_id) {
    STREAMING_LOG(WARNING) << "Can not notify consumed this offset " << offset
                           << " that's out of range, max seq id "
                           << channel_info.current_seq_id;
  } else {
    channel_map_[channel_info.channel_id]->NotifyChannelConsumed(offset);
  }
}

void DataWriter::FlowControlTimer() {
  std::chrono::milliseconds MockTimer(
      runtime_context_->GetConfig().GetEventDrivenFlowControlInterval());
  while (true) {
    if (runtime_context_->GetRuntimeStatus() != RuntimeStatus::Running) {
      return;
    }
    for (const auto &output_queue : output_queue_ids_) {
      if (runtime_context_->GetRuntimeStatus() != RuntimeStatus::Running) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (!channel_info.flow_control) {
        continue;
      }
      if (!flow_controller_->ShouldFlowControl(channel_info)) {
        channel_info.flow_control = false;
        Event event{&channel_info, EventType::FlowEvent,
                    channel_info.writer_ring_buffer->IsFull()};
        event_service_->Push(event);
        ++channel_info.flow_control_cnt;
      }
    }
    std::this_thread::sleep_for(MockTimer);
  }
}

void DataWriter::GetOffsetInfo(
    std::unordered_map<ObjectID, ProducerChannelInfo> *&offset_map) {
  offset_map = &channel_info_map_;
}

}  // namespace streaming
}  // namespace ray
