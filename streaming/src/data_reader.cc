#include "data_reader.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

#include "message/message_bundle.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace streaming {

const uint32_t DataReader::kReadItemTimeout = 1000;

void DataReader::Init(const std::vector<ObjectID> &input_ids,
                      const std::vector<ChannelCreationParameter> &init_params,
                      const std::vector<uint64_t> &queue_seq_ids,
                      const std::vector<uint64_t> &streaming_msg_ids,
                      int64_t timer_interval) {
  Init(input_ids, init_params, timer_interval);
  for (size_t i = 0; i < input_ids.size(); ++i) {
    auto &q_id = input_ids[i];
    channel_info_map_[q_id].current_seq_id = queue_seq_ids[i];
    channel_info_map_[q_id].current_message_id = streaming_msg_ids[i];
  }
}

void DataReader::Init(const std::vector<ObjectID> &input_ids,
                      const std::vector<ChannelCreationParameter> &init_params,
                      int64_t timer_interval) {
  STREAMING_LOG(INFO) << input_ids.size() << " queue to init.";

  transfer_config_->Set(ConfigEnum::QUEUE_ID_VECTOR, input_ids);

  last_fetched_queue_item_ = nullptr;
  timer_interval_ = timer_interval;
  last_message_ts_ = 0;
  input_queue_ids_ = input_ids;
  last_message_latency_ = 0;
  last_bundle_unit_ = 0;

  for (size_t i = 0; i < input_ids.size(); ++i) {
    ObjectID q_id = input_ids[i];
    STREAMING_LOG(INFO) << "[Reader] Init queue id: " << q_id;
    auto &channel_info = channel_info_map_[q_id];
    channel_info.channel_id = q_id;
    channel_info.parameter = init_params[i];
    channel_info.last_queue_item_delay = 0;
    channel_info.last_queue_item_latency = 0;
    channel_info.last_queue_target_diff = 0;
    channel_info.get_queue_item_times = 0;
  }

  /// Make the input id location stable.
  sort(input_queue_ids_.begin(), input_queue_ids_.end(),
       [](const ObjectID &a, const ObjectID &b) { return a.Hash() < b.Hash(); });
  std::copy(input_ids.begin(), input_ids.end(), std::back_inserter(unready_queue_ids_));
  InitChannel();
}

StreamingStatus DataReader::InitChannel() {
  STREAMING_LOG(INFO) << "[Reader] Getting queues. total queue num "
                      << input_queue_ids_.size() << ", unready queue num => "
                      << unready_queue_ids_.size();

  for (const auto &input_channel : unready_queue_ids_) {
    auto &channel_info = channel_info_map_[input_channel];
    std::shared_ptr<ConsumerChannel> channel;
    if (runtime_context_->IsMockTest()) {
      channel = std::make_shared<MockConsumer>(transfer_config_, channel_info);
    } else {
      channel = std::make_shared<StreamingQueueConsumer>(transfer_config_, channel_info);
    }

    channel_map_.emplace(input_channel, channel);
    StreamingStatus status = channel->CreateTransferChannel();
    if (StreamingStatus::OK != status) {
      STREAMING_LOG(ERROR) << "Initialize queue failed, id => " << input_channel;
    }
  }
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Running);
  STREAMING_LOG(INFO) << "[Reader] Reader construction done!";
  return StreamingStatus::OK;
}

StreamingStatus DataReader::InitChannelMerger() {
  STREAMING_LOG(INFO) << "[Reader] Initializing queue merger.";
  // Init reader merger by given comparator when it's first created.
  StreamingReaderMsgPtrComparator comparator;
  if (!reader_merger_) {
    reader_merger_.reset(
        new PriorityQueue<std::shared_ptr<DataBundle>, StreamingReaderMsgPtrComparator>(
            comparator));
  }

  // An old item in merger vector must be evicted before new queue item has been
  // pushed.
  if (!unready_queue_ids_.empty() && last_fetched_queue_item_) {
    STREAMING_LOG(INFO) << "pop old item from => " << last_fetched_queue_item_->from;
    RETURN_IF_NOT_OK(StashNextMessage(last_fetched_queue_item_))
    last_fetched_queue_item_.reset();
  }
  // Create initial heap for priority queue.
  for (auto &input_queue : unready_queue_ids_) {
    std::shared_ptr<DataBundle> msg = std::make_shared<DataBundle>();
    RETURN_IF_NOT_OK(GetMessageFromChannel(channel_info_map_[input_queue], msg))
    channel_info_map_[msg->from].current_seq_id = msg->seq_id;
    channel_info_map_[msg->from].current_message_id = msg->meta->GetLastMessageId();
    reader_merger_->push(msg);
  }
  STREAMING_LOG(INFO) << "[Reader] Initializing merger done.";
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetMessageFromChannel(ConsumerChannelInfo &channel_info,
                                                  std::shared_ptr<DataBundle> &message) {
  auto &qid = channel_info.channel_id;
  last_read_q_id_ = qid;
  STREAMING_LOG(DEBUG) << "[Reader] send get request queue seq id => " << qid;
  while (RuntimeStatus::Running == runtime_context_->GetRuntimeStatus() &&
         !message->data) {
    auto status = channel_map_[channel_info.channel_id]->ConsumeItemFromChannel(
        message->seq_id, message->data, message->data_size, kReadItemTimeout);
    channel_info.get_queue_item_times++;
    if (!message->data) {
      STREAMING_LOG(DEBUG) << "[Reader] Queue " << qid << " status " << status
                           << " get item timeout, resend notify "
                           << channel_info.current_seq_id;
      // TODO(lingxuan.zlx): notify consumed when it's timeout.
    }
  }
  if (RuntimeStatus::Interrupted == runtime_context_->GetRuntimeStatus()) {
    return StreamingStatus::Interrupted;
  }
  STREAMING_LOG(DEBUG) << "[Reader] recevied queue seq id => " << message->seq_id
                       << ", queue id => " << qid;

  message->from = qid;
  message->meta = StreamingMessageBundleMeta::FromBytes(message->data);
  return StreamingStatus::OK;
}

StreamingStatus DataReader::StashNextMessage(std::shared_ptr<DataBundle> &message) {
  // Push new message into priority queue and record the channel metrics in
  // channel info.
  std::shared_ptr<DataBundle> new_msg = std::make_shared<DataBundle>();
  auto &channel_info = channel_info_map_[message->from];
  reader_merger_->pop();
  int64_t cur_time = current_time_ms();
  RETURN_IF_NOT_OK(GetMessageFromChannel(channel_info, new_msg))
  reader_merger_->push(new_msg);
  channel_info.last_queue_item_delay =
      new_msg->meta->GetMessageBundleTs() - message->meta->GetMessageBundleTs();
  channel_info.last_queue_item_latency = current_time_ms() - cur_time;
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetMergedMessageBundle(std::shared_ptr<DataBundle> &message,
                                                   bool &is_valid_break) {
  int64_t cur_time = current_time_ms();
  if (last_fetched_queue_item_) {
    RETURN_IF_NOT_OK(StashNextMessage(last_fetched_queue_item_))
  }
  message = reader_merger_->top();
  last_fetched_queue_item_ = message;
  auto &offset_info = channel_info_map_[message->from];

  uint64_t cur_queue_previous_msg_id = offset_info.current_message_id;
  STREAMING_LOG(DEBUG) << "[Reader] [Bundle] from q_id =>" << message->from << "cur => "
                       << cur_queue_previous_msg_id << ", message list size"
                       << message->meta->GetMessageListSize() << ", lst message id =>"
                       << message->meta->GetLastMessageId() << ", q seq id => "
                       << message->seq_id << ", last barrier id => " << message->data_size
                       << ", " << message->meta->GetMessageBundleTs();

  if (message->meta->IsBundle()) {
    last_message_ts_ = cur_time;
    is_valid_break = true;
  } else if (timer_interval_ != -1 && cur_time - last_message_ts_ > timer_interval_) {
    // Throw empty message when reaching timer_interval.
    last_message_ts_ = cur_time;
    is_valid_break = true;
  }

  offset_info.current_message_id = message->meta->GetLastMessageId();
  offset_info.current_seq_id = message->seq_id;
  last_bundle_ts_ = message->meta->GetMessageBundleTs();

  STREAMING_LOG(DEBUG) << "[Reader] [Bundle] message type =>"
                       << static_cast<int>(message->meta->GetBundleType())
                       << " from id => " << message->from << ", queue seq id =>"
                       << message->seq_id << ", message id => "
                       << message->meta->GetLastMessageId();
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetBundle(const uint32_t timeout_ms,
                                      std::shared_ptr<DataBundle> &message) {
  // Notify upstream that last fetched item has been consumed.
  if (last_fetched_queue_item_) {
    NotifyConsumed(last_fetched_queue_item_);
  }

  /// DataBundle will be returned to the upper layer in the following cases:
  /// a batch of data is returned when the real data is read, or an empty message
  /// is returned to the upper layer when the given timeout period is reached to
  /// avoid blocking for too long.
  auto start_time = current_time_ms();
  bool is_valid_break = false;
  uint32_t empty_bundle_cnt = 0;
  while (!is_valid_break) {
    if (RuntimeStatus::Interrupted == runtime_context_->GetRuntimeStatus()) {
      return StreamingStatus::Interrupted;
    }
    auto cur_time = current_time_ms();
    auto dur = cur_time - start_time;
    if (dur > timeout_ms) {
      return StreamingStatus::GetBundleTimeOut;
    }
    if (!unready_queue_ids_.empty()) {
      StreamingStatus status = InitChannel();
      switch (status) {
      case StreamingStatus::InitQueueFailed:
        break;
      case StreamingStatus::WaitQueueTimeOut:
        STREAMING_LOG(ERROR)
            << "Wait upstream queue timeout, maybe some actors in deadlock";
        break;
      default:
        STREAMING_LOG(INFO) << "Init reader queue in GetBundle";
      }
      if (StreamingStatus::OK != status) {
        return status;
      }
      RETURN_IF_NOT_OK(InitChannelMerger())
      unready_queue_ids_.clear();
      auto &merge_vec = reader_merger_->getRawVector();
      for (auto &bundle : merge_vec) {
        STREAMING_LOG(INFO) << "merger vector item => " << bundle->from;
      }
    }
    RETURN_IF_NOT_OK(GetMergedMessageBundle(message, is_valid_break));
    if (!is_valid_break) {
      empty_bundle_cnt++;
      NotifyConsumed(message);
    }
  }
  last_message_latency_ += current_time_ms() - start_time;
  if (message->meta->GetMessageListSize() > 0) {
    last_bundle_unit_ = message->data_size * 1.0 / message->meta->GetMessageListSize();
  }
  return StreamingStatus::OK;
}

void DataReader::GetOffsetInfo(
    std::unordered_map<ObjectID, ConsumerChannelInfo> *&offset_map) {
  offset_map = &channel_info_map_;
  for (auto &offset_info : channel_info_map_) {
    STREAMING_LOG(INFO) << "[Reader] [GetOffsetInfo], q id " << offset_info.first
                        << ", seq id => " << offset_info.second.current_seq_id
                        << ", message id => " << offset_info.second.current_message_id;
  }
}

void DataReader::NotifyConsumedItem(ConsumerChannelInfo &channel_info, uint64_t offset) {
  channel_map_[channel_info.channel_id]->NotifyChannelConsumed(offset);
  if (offset == channel_info.queue_info.last_seq_id) {
    STREAMING_LOG(DEBUG) << "notify seq id equal to last seq id => " << offset;
  }
}

DataReader::DataReader(std::shared_ptr<RuntimeContext> &runtime_context)
    : transfer_config_(new Config()), runtime_context_(runtime_context) {}

DataReader::~DataReader() { STREAMING_LOG(INFO) << "Streaming reader deconstruct."; }

void DataReader::Stop() {
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Interrupted);
}

void DataReader::NotifyConsumed(std::shared_ptr<DataBundle> &message) {
  auto &channel_info = channel_info_map_[message->from];
  auto &queue_info = channel_info.queue_info;
  channel_info.notify_cnt++;
  if (queue_info.target_seq_id <= message->seq_id) {
    NotifyConsumedItem(channel_info, message->seq_id);

    channel_map_[channel_info.channel_id]->RefreshChannelInfo();
    if (queue_info.last_seq_id != QUEUE_INVALID_SEQ_ID) {
      uint64_t original_target_seq_id = queue_info.target_seq_id;
      queue_info.target_seq_id = std::min(
          queue_info.last_seq_id,
          message->seq_id + runtime_context_->GetConfig().GetReaderConsumedStep());
      channel_info.last_queue_target_diff =
          queue_info.target_seq_id - original_target_seq_id;
    } else {
      STREAMING_LOG(WARNING) << "[Reader] [QueueInfo] channel id " << message->from
                             << ", last seq id " << queue_info.last_seq_id;
    }
    STREAMING_LOG(DEBUG) << "[Reader] [Consumed] Trigger notify consumed"
                         << ", channel id => " << message->from << ", last seq id => "
                         << queue_info.last_seq_id << ", target seq id => "
                         << queue_info.target_seq_id << ", consumed seq id => "
                         << message->seq_id << ", last message id => "
                         << message->meta->GetLastMessageId() << ", bundle type => "
                         << static_cast<uint32_t>(message->meta->GetBundleType())
                         << ", last message bundle ts => "
                         << message->meta->GetMessageBundleTs();
  }
}

bool StreamingReaderMsgPtrComparator::operator()(const std::shared_ptr<DataBundle> &a,
                                                 const std::shared_ptr<DataBundle> &b) {
  STREAMING_CHECK(a->meta);
  // We use hash value of id for stability of message in sorting.
  if (a->meta->GetMessageBundleTs() == b->meta->GetMessageBundleTs()) {
    return a->from.Hash() > b->from.Hash();
  }
  return a->meta->GetMessageBundleTs() > b->meta->GetMessageBundleTs();
}

}  // namespace streaming

}  // namespace ray
