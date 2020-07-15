#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

#include "ray/util/logging.h"
#include "ray/util/util.h"

#include "data_reader.h"
#include "message/message_bundle.h"

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
    last_message_id_[q_id] = streaming_msg_ids[i];
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
    channel_info.resend_notify_timer = 0;
  }

  reliability_helper_ = ReliabilityHelperFactory::GenReliabilityHelper(
      runtime_context_->GetConfig(), barrier_helper_, nullptr, this);

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
    TransferCreationStatus status = channel->CreateTransferChannel();
    if (TransferCreationStatus::PullOk != status) {
      STREAMING_LOG(ERROR) << "Initialize queue failed, id => " << input_channel;
    }
  }
  runtime_context_->SetRuntimeStatus(RuntimeStatus::Running);
  STREAMING_LOG(INFO) << "[Reader] Reader construction done!";
  return StreamingStatus::OK;
}

StreamingStatus DataReader::InitChannelMerger(uint32_t timeout_ms) {
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
    RETURN_IF_NOT_OK(StashNextMessage(last_fetched_queue_item_, timeout_ms))
    last_fetched_queue_item_.reset();
  }
  // Create initial heap for priority queue.
  for (auto &input_queue : unready_queue_ids_) {
    std::shared_ptr<DataBundle> msg = std::make_shared<DataBundle>();
    RETURN_IF_NOT_OK(GetMessageFromChannel(channel_info_map_[input_queue], msg, timeout_ms, timeout_ms))
    channel_info_map_[msg->from].current_message_id = msg->meta->GetLastMessageId();
    reader_merger_->push(msg);
  }
  STREAMING_LOG(INFO) << "[Reader] Initializing merger done.";
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetMessageFromChannel(
    ConsumerChannelInfo &channel_info, std::shared_ptr<DataBundle> &message,
    uint32_t timeout_ms, uint32_t wait_time_ms) {
  auto &qid = channel_info.channel_id;
  message->from = qid;
  last_read_q_id_ = qid;

  bool is_valid_bundle = false;
  int64_t start_time = current_sys_time_ms();
  STREAMING_LOG(DEBUG) << "GetMessageFromChannel, timeout_ms=" << timeout_ms
                       << ", wait_time_ms=" << wait_time_ms;
  while (runtime_context_->GetRuntimeStatus() == RuntimeStatus::Running && !is_valid_bundle &&
         current_sys_time_ms() - start_time < timeout_ms) {
    STREAMING_LOG(DEBUG) << "[Reader] send get request queue seq id => " << qid;
    /// In AT_LEAST_ONCE, wait_time_ms is set to 0, means `ConsumeItemFromChannel`
    /// will return immediately if no items in queue. At the same time, `timeout_ms` is
    /// ignored.
    channel_map_[channel_info.channel_id]->ConsumeItemFromChannel(message, wait_time_ms);

    channel_info.get_queue_item_times++;
    if (!message->data) {
      RETURN_IF_NOT_OK(reliability_helper_->HandleNoValidItem(channel_info));
    } else {
      uint64_t current_time = current_sys_time_ms();
      channel_info.resend_notify_timer = current_time;
      // Note(lingxuan.zlx): To find which channel get an invalid data and
      // print channel id for debugging.
      STREAMING_CHECK(
          StreamingMessageBundleMeta::CheckBundleMagicNum(message->data))
          << "Magic number invalid, from channel " << channel_info.channel_id;
      message->meta = StreamingMessageBundleMeta::FromBytes(message->data);

      is_valid_bundle = true;
      if (!runtime_context_->GetConfig().IsAtLeastOnce()) {
        // filter message when msg_id doesn't match.
        // reader will filter message only when using streaming queue and
        // non-at-least-once mode
        BundleCheckStatus status = CheckBundle(message);
        STREAMING_LOG(DEBUG) << "CheckBundle, result=" << status
                             << ", last_msg_id=" << last_message_id_[message->from];
        if (status == BundleCheckStatus::BundleToBeSplit) {
          SplitBundle(message, last_message_id_[qid]);
        }
        if (status == BundleCheckStatus::BundleToBeThrown && message->meta->IsBarrier()) {
          STREAMING_LOG(WARNING)
              << "Throw barrier, msg_id=" << message->meta->GetLastMessageId();
        }
        is_valid_bundle = status != BundleCheckStatus::BundleToBeThrown;
      }
    }
  }
  if (RuntimeStatus::Interrupted == runtime_context_->GetRuntimeStatus()) {
    return StreamingStatus::Interrupted;
  }

  if (!is_valid_bundle) {
    STREAMING_LOG(DEBUG) << "GetMessageFromChannel timeout, qid="
                         << channel_info.channel_id;
    return StreamingStatus::GetBundleTimeOut;
  }

  STREAMING_LOG(DEBUG) << "[Reader] received queue seq id => " << message->seq_id
                       << ", queue id => " << qid;
  last_message_id_[message->from] = message->meta->GetLastMessageId();
  return StreamingStatus::OK;
}

BundleCheckStatus DataReader::CheckBundle(
    const std::shared_ptr<DataBundle> &message) {
  uint64_t end_msg_id = message->meta->GetLastMessageId();
  uint64_t start_msg_id = message->meta->IsEmptyMsg()
                              ? end_msg_id
                              : end_msg_id - message->meta->GetMessageListSize() + 1;
  uint64_t last_msg_id = last_message_id_[message->from];

  // writer will keep sending bundles when downstream reader failover. After reader
  // recovered, it will receive these bundles whoes msg_id is larger than expected.
  if (start_msg_id > last_msg_id + 1) {
    return BundleCheckStatus::BundleToBeThrown;
  }
  if (end_msg_id < last_msg_id + 1) {
    // empty message and barrier's msg_id equals to last message, so we shouldn't throw
    // them.
    return end_msg_id == last_msg_id && !message->meta->IsBundle()
               ? BundleCheckStatus::OkBundle
               : BundleCheckStatus::BundleToBeThrown;
  }
  // normal bundles
  if (start_msg_id == last_msg_id + 1) {
    return BundleCheckStatus::OkBundle;
  }
  return BundleCheckStatus::BundleToBeSplit;
}

void DataReader::SplitBundle(std::shared_ptr<DataBundle> &message,
                             uint64_t last_msg_id) {
  std::list<StreamingMessagePtr> msg_list;
  StreamingMessageBundle::GetMessageListFromRawData(
      message->data, message->data_size,
      message->meta->GetMessageListSize(), msg_list);
  uint64_t bundle_size = 0;
  for (auto it = msg_list.begin(); it != msg_list.end();) {
    if ((*it)->GetMessageSeqId() > last_msg_id) {
      bundle_size += (*it)->ClassBytesSize();
      it++;
    } else {
      it = msg_list.erase(it);
    }
  }
  STREAMING_LOG(DEBUG) << "Split message, from_queue_id=" << message->from
                       << ", start_msg_id=" << msg_list.front()->GetMessageSeqId()
                       << ", end_msg_id=" << msg_list.back()->GetMessageSeqId();
  // recreate bundle
  auto cut_msg_bundle = std::make_shared<StreamingMessageBundle>(
      msg_list, message->meta->GetMessageBundleTs(), msg_list.back()->GetMessageSeqId(),
      StreamingMessageBundleType::Bundle, bundle_size);
  message->Realloc(cut_msg_bundle->ClassBytesSize());
  cut_msg_bundle->ToBytes(message->data);
  message->meta = StreamingMessageBundleMeta::FromBytes(message->data);
}

StreamingStatus DataReader::StashNextMessage(std::shared_ptr<DataBundle> &message, uint32_t timeout_ms) {
  // Push new message into priority queue and record the channel metrics in
  // channel info.
  std::shared_ptr<DataBundle> new_msg = std::make_shared<DataBundle>();
  auto &channel_info = channel_info_map_[message->from];
  reader_merger_->pop();
  int64_t cur_time = current_time_ms();
  RETURN_IF_NOT_OK(GetMessageFromChannel(channel_info, new_msg, timeout_ms, timeout_ms))
  reader_merger_->push(new_msg);
  channel_info.last_queue_item_delay =
      new_msg->meta->GetMessageBundleTs() - message->meta->GetMessageBundleTs();
  channel_info.last_queue_item_latency = current_time_ms() - cur_time;
  return StreamingStatus::OK;
}

StreamingStatus DataReader::GetMergedMessageBundle(std::shared_ptr<DataBundle> &message,
                                                   bool &is_valid_break, uint32_t timeout_ms) {
  int64_t cur_time = current_time_ms();
  if (last_fetched_queue_item_) {
    RETURN_IF_NOT_OK(StashNextMessage(last_fetched_queue_item_, timeout_ms))
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
      RETURN_IF_NOT_OK(InitChannelMerger(timeout_ms))
      unready_queue_ids_.clear();
      auto &merge_vec = reader_merger_->getRawVector();
      for (auto &bundle : merge_vec) {
        STREAMING_LOG(INFO) << "merger vector item => " << bundle->from;
      }
    }
    RETURN_IF_NOT_OK(GetMergedMessageBundle(message, is_valid_break, timeout_ms));
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
