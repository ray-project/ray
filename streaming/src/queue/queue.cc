#include "queue/queue.h"

#include <chrono>
#include <thread>

#include "queue/queue_handler.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

bool Queue::Push(QueueItem item) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (max_data_size_ < item.DataSize() + data_size_) return false;

  buffer_queue_.push_back(item);
  data_size_ += item.DataSize();
  readable_cv_.notify_one();
  return true;
}

QueueItem Queue::FrontProcessed() {
  std::unique_lock<std::mutex> lock(mutex_);
  STREAMING_CHECK(buffer_queue_.size() != 0) << "WriterQueue Pop fail";

  if (watershed_iter_ == buffer_queue_.begin()) {
    return InvalidQueueItem();
  }

  QueueItem item = buffer_queue_.front();
  return item;
}

QueueItem Queue::PopProcessed() {
  std::unique_lock<std::mutex> lock(mutex_);
  STREAMING_CHECK(buffer_queue_.size() != 0) << "WriterQueue Pop fail";

  if (watershed_iter_ == buffer_queue_.begin()) {
    return InvalidQueueItem();
  }

  QueueItem item = buffer_queue_.front();
  buffer_queue_.pop_front();
  data_size_ -= item.DataSize();
  data_size_sent_ -= item.DataSize();
  return item;
}

QueueItem Queue::PopPending() {
  std::unique_lock<std::mutex> lock(mutex_);
  auto it = std::next(watershed_iter_);
  QueueItem item = *it;
  data_size_sent_ += it->DataSize();
  buffer_queue_.splice(watershed_iter_, buffer_queue_, it, std::next(it));
  return item;
}

QueueItem Queue::PopPendingBlockTimeout(uint64_t timeout_us) {
  std::unique_lock<std::mutex> lock(mutex_);
  std::chrono::system_clock::time_point point =
      std::chrono::system_clock::now() + std::chrono::microseconds(timeout_us);
  if (readable_cv_.wait_until(lock, point, [this] {
        return std::next(watershed_iter_) != buffer_queue_.end();
      })) {
    auto it = std::next(watershed_iter_);
    QueueItem item = *it;
    data_size_sent_ += it->DataSize();
    buffer_queue_.splice(watershed_iter_, buffer_queue_, it, std::next(it));
    return item;

  } else {
    return InvalidQueueItem();
  }
}

QueueItem Queue::BackPending() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) {
    return InvalidQueueItem();
  }
  return buffer_queue_.back();
}

size_t Queue::ProcessedCount() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (watershed_iter_ == buffer_queue_.begin()) return 0;

  auto begin = buffer_queue_.begin();
  auto end = std::prev(watershed_iter_);

  return end->SeqId() + 1 - begin->SeqId();
}

size_t Queue::PendingCount() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) return 0;

  auto begin = std::next(watershed_iter_);
  auto end = std::prev(buffer_queue_.end());

  return begin->SeqId() - end->SeqId() + 1;
}

Status WriterQueue::Push(uint8_t *buffer, uint32_t buffer_size, uint64_t timestamp,
                         uint64_t msg_id_start, uint64_t msg_id_end, bool raw) {
  if (IsPendingFull(buffer_size)) {
    return Status::OutOfMemory("Queue Push OutOfMemory");
  }

  while (is_resending_) {
    STREAMING_LOG(INFO) << "This queue is resending data, wait.";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  QueueItem item(seq_id_, buffer, buffer_size, timestamp, msg_id_start, msg_id_end, raw);
  Queue::Push(item);
  STREAMING_LOG(DEBUG) << "WriterQueue::Push seq_id: " << seq_id_;
  seq_id_++;
  return Status::OK();
}

void WriterQueue::Send() {
  while (!IsPendingEmpty()) {
    QueueItem item = PopPending();
    DataMessage msg(actor_id_, peer_actor_id_, queue_id_, item.SeqId(), item.MsgIdStart(),
                    item.MsgIdEnd(), item.Buffer(), item.IsRaw());
    std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    STREAMING_CHECK(transport_ != nullptr);
    transport_->Send(std::move(buffer));
  }
}

Status WriterQueue::TryEvictItems() {
  QueueItem item = FrontProcessed();
  STREAMING_LOG(DEBUG) << "TryEvictItems queue_id: " << queue_id_ << " first_item: ("
                       << item.MsgIdStart() << "," << item.MsgIdEnd() << ")"
                       << " min_consumed_msg_id_: " << min_consumed_msg_id_
                       << " eviction_limit_: " << eviction_limit_
                       << " max_data_size_: " << max_data_size_
                       << " data_size_sent_: " << data_size_sent_
                       << " data_size_: " << data_size_;

  if (min_consumed_msg_id_ == QUEUE_INVALID_SEQ_ID ||
      min_consumed_msg_id_ < item.MsgIdEnd()) {
    return Status::OutOfMemory("The queue is full and some reader doesn't consume");
  }

  if (eviction_limit_ == QUEUE_INVALID_SEQ_ID || eviction_limit_ < item.MsgIdEnd()) {
    return Status::OutOfMemory("The queue is full and eviction limit block evict");
  }

  uint64_t evict_target_msg_id = std::min(min_consumed_msg_id_, eviction_limit_);

  int count = 0;
  while (item.MsgIdEnd() <= evict_target_msg_id) {
    PopProcessed();
    STREAMING_LOG(INFO) << "TryEvictItems directly " << item.MsgIdEnd();
    item = FrontProcessed();
    count++;
  }
  STREAMING_LOG(DEBUG) << count << " items evicted, current item: (" << item.MsgIdStart()
                       << "," << item.MsgIdEnd() << ")";
  return Status::OK();
}

void WriterQueue::OnNotify(std::shared_ptr<NotificationMessage> notify_msg) {
  STREAMING_LOG(INFO) << "OnNotify target msg_id: " << notify_msg->MsgId();
  min_consumed_msg_id_ = notify_msg->MsgId();
}

void WriterQueue::ResendItem(QueueItem &item, uint64_t first_seq_id,
                             uint64_t last_seq_id) {
  ResendDataMessage msg(actor_id_, peer_actor_id_, queue_id_, first_seq_id, item.SeqId(),
                        item.MsgIdStart(), item.MsgIdEnd(), last_seq_id, item.Buffer(),
                        item.IsRaw());
  STREAMING_CHECK(item.Buffer()->Data() != nullptr);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  transport_->Send(std::move(buffer));
}

int WriterQueue::ResendItems(std::list<QueueItem>::iterator start_iter,
                             uint64_t first_seq_id, uint64_t last_seq_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  int count = 0;
  auto it = start_iter;
  for (; it != watershed_iter_; it++) {
    if (it->SeqId() > last_seq_id) {
      break;
    }
    STREAMING_LOG(INFO) << "ResendItems send seq_id " << it->SeqId() << " to peer.";
    ResendItem(*it, first_seq_id, last_seq_id);
    count++;
  }

  STREAMING_LOG(INFO) << "ResendItems total count: " << count;
  is_resending_ = false;
  return count;
}

void WriterQueue::FindItem(
    uint64_t target_msg_id, std::function<void()> greater_callback,
    std::function<void()> less_callback,
    std::function<void(std::list<QueueItem>::iterator, uint64_t, uint64_t)>
        equal_callback) {
  auto last_one = std::prev(watershed_iter_);
  bool last_item_too_small =
      last_one != buffer_queue_.end() && last_one->MsgIdEnd() < target_msg_id;

  if (QUEUE_INITIAL_SEQ_ID == seq_id_ || last_item_too_small) {
    greater_callback();
    return;
  }

  auto begin = buffer_queue_.begin();
  uint64_t first_seq_id = (*begin).SeqId();
  uint64_t last_seq_id = first_seq_id + std::distance(begin, watershed_iter_) - 1;
  STREAMING_LOG(INFO) << "FindItem last_seq_id: " << last_seq_id
                      << " first_seq_id: " << first_seq_id;

  auto target_item = std::find_if(
      begin, watershed_iter_,
      [&target_msg_id](QueueItem &item) { return item.InItem(target_msg_id); });

  if (target_item != watershed_iter_) {
    equal_callback(target_item, first_seq_id, last_seq_id);
  } else {
    less_callback();
  }
}

void WriterQueue::OnPull(
    std::shared_ptr<PullRequestMessage> pull_msg, boost::asio::io_service &service,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::unique_lock<std::mutex> lock(mutex_);
  STREAMING_CHECK(peer_actor_id_ == pull_msg->ActorId())
      << peer_actor_id_ << " " << pull_msg->ActorId();

  FindItem(pull_msg->MsgId(),
           /// target_msg_id is too large.
           [this, &pull_msg, &callback]() {
             STREAMING_LOG(WARNING)
                 << "No valid data to pull, the writer has not push data yet. ";
             PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                                     pull_msg->QueueId(), QUEUE_INVALID_SEQ_ID,
                                     QUEUE_INVALID_SEQ_ID,
                                     queue::protobuf::StreamingQueueError::NO_VALID_DATA,
                                     is_upstream_first_pull_);
             std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
             is_upstream_first_pull_ = false;
             callback(std::move(buffer));
           },
           /// target_msg_id is too small.
           [this, &pull_msg, &callback]() {
             STREAMING_LOG(WARNING) << "Data lost.";
             PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                                     pull_msg->QueueId(), QUEUE_INVALID_SEQ_ID,
                                     QUEUE_INVALID_SEQ_ID,
                                     queue::protobuf::StreamingQueueError::DATA_LOST,
                                     is_upstream_first_pull_);
             std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

             callback(std::move(buffer));
           },
           /// target_msg_id found.
           [this, &pull_msg, &callback, &service](
               std::list<QueueItem>::iterator target_item, uint64_t first_seq_id,
               uint64_t last_seq_id) {
             is_resending_ = true;
             STREAMING_LOG(INFO) << "OnPull return";
             service.post(std::bind(&WriterQueue::ResendItems, this, target_item,
                                    first_seq_id, last_seq_id));
             PullResponseMessage msg(
                 pull_msg->PeerActorId(), pull_msg->ActorId(), pull_msg->QueueId(),
                 target_item->SeqId(), pull_msg->MsgId(),
                 queue::protobuf::StreamingQueueError::OK, is_upstream_first_pull_);
             std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
             is_upstream_first_pull_ = false;
             callback(std::move(buffer));
           });
}

void ReaderQueue::OnConsumed(uint64_t msg_id) {
  STREAMING_LOG(INFO) << "OnConsumed: " << msg_id;
  QueueItem item = FrontProcessed();
  while (item.MsgIdEnd() <= msg_id) {
    PopProcessed();
    item = FrontProcessed();
  }
  Notify(msg_id);
}

void ReaderQueue::Notify(uint64_t msg_id) {
  std::vector<TaskArg> task_args;
  CreateNotifyTask(msg_id, task_args);
  // SubmitActorTask

  NotificationMessage msg(actor_id_, peer_actor_id_, queue_id_, msg_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  transport_->Send(std::move(buffer));
}

void ReaderQueue::CreateNotifyTask(uint64_t seq_id, std::vector<TaskArg> &task_args) {}

void ReaderQueue::OnData(QueueItem &item) {
  last_recv_seq_id_ = item.SeqId();
  last_recv_msg_id_ = item.MsgIdEnd();
  STREAMING_LOG(DEBUG) << "ReaderQueue::OnData queue_id: " << queue_id_
                       << " seq_id: " << last_recv_seq_id_ << " msg_id: ("
                       << item.MsgIdStart() << "," << item.MsgIdEnd() << ")";

  Push(item);
}

void ReaderQueue::OnResendData(std::shared_ptr<ResendDataMessage> msg) {
  STREAMING_LOG(INFO) << "OnResendData queue_id: " << queue_id_ << " recv seq_id "
                      << msg->SeqId() << "(" << msg->FirstSeqId() << "/"
                      << msg->LastSeqId() << ")";
  QueueItem item(msg->SeqId(), msg->Buffer(), 0, msg->MsgIdStart(), msg->MsgIdEnd(),
                 msg->IsRaw());
  STREAMING_CHECK(msg->Buffer()->Data() != nullptr);

  Push(item);
  STREAMING_CHECK(msg->SeqId() >= msg->FirstSeqId() && msg->SeqId() <= msg->LastSeqId())
      << "(" << msg->FirstSeqId() << "/" << msg->SeqId() << "/" << msg->LastSeqId()
      << ")";
  if (msg->SeqId() == msg->LastSeqId()) {
    STREAMING_LOG(INFO) << "Resend DATA Done";
  }
}

}  // namespace streaming
}  // namespace ray
