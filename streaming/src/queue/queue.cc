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
    uint8_t data[1];
    return QueueItem(QUEUE_INVALID_SEQ_ID, data, 1, 0, true);
  }
}

QueueItem Queue::BackPending() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) {
    uint8_t data[1];
    return QueueItem(QUEUE_INVALID_SEQ_ID, data, 1, 0, true);
  }
  return buffer_queue_.back();
}

bool Queue::IsPendingEmpty() {
  std::unique_lock<std::mutex> lock(mutex_);
  return std::next(watershed_iter_) == buffer_queue_.end();
}

bool Queue::IsPendingFull(uint64_t data_size) {
  std::unique_lock<std::mutex> lock(mutex_);
  return max_data_size_ < data_size + data_size_;
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

Status WriterQueue::Push(uint64_t seq_id, uint8_t *data, uint32_t data_size,
                         uint64_t timestamp, bool raw) {
  if (IsPendingFull(data_size)) {
    return Status::OutOfMemory("Queue Push OutOfMemory");
  }

  while (is_pulling_) {
    STREAMING_LOG(INFO) << "This queue is sending pull data, wait.";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  QueueItem item(seq_id, data, data_size, timestamp, raw);
  Queue::Push(item);
  return Status::OK();
}

void WriterQueue::Send() {
  while (!IsPendingEmpty()) {
    // FIXME: front -> send -> pop
    QueueItem item = PopPending();
    DataMessage msg(actor_id_, peer_actor_id_, queue_id_, item.SeqId(), item.Buffer(),
                    item.IsRaw());
    std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    STREAMING_CHECK(transport_ != nullptr);
    transport_->Send(std::move(buffer));
  }
}

Status WriterQueue::TryEvictItems() {
  STREAMING_LOG(INFO) << "TryEvictItems";
  QueueItem item = FrontProcessed();
  uint64_t first_seq_id = item.SeqId();
  STREAMING_LOG(INFO) << "TryEvictItems first_seq_id: " << first_seq_id
                      << " min_consumed_id_: " << min_consumed_id_
                      << " eviction_limit_: " << eviction_limit_;
  if (min_consumed_id_ == QUEUE_INVALID_SEQ_ID || first_seq_id > min_consumed_id_) {
    return Status::OutOfMemory("The queue is full and some reader doesn't consume");
  }

  if (eviction_limit_ == QUEUE_INVALID_SEQ_ID || first_seq_id > eviction_limit_) {
    return Status::OutOfMemory("The queue is full and eviction limit block evict");
  }

  uint64_t evict_target_seq_id = std::min(min_consumed_id_, eviction_limit_);

  while (item.SeqId() <= evict_target_seq_id) {
    PopProcessed();
    STREAMING_LOG(INFO) << "TryEvictItems directly " << item.SeqId();
    item = FrontProcessed();
  }
  return Status::OK();
}

void WriterQueue::OnNotify(std::shared_ptr<NotificationMessage> notify_msg) {
  STREAMING_LOG(INFO) << "OnNotify target seq_id: " << notify_msg->SeqId();
  min_consumed_id_ = notify_msg->SeqId();
}

void ReaderQueue::OnConsumed(uint64_t seq_id) {
  STREAMING_LOG(INFO) << "OnConsumed: " << seq_id;
  QueueItem item = FrontProcessed();
  while (item.SeqId() <= seq_id) {
    PopProcessed();
    item = FrontProcessed();
  }
  Notify(seq_id);
}

void ReaderQueue::Notify(uint64_t seq_id) {
  std::vector<TaskArg> task_args;
  CreateNotifyTask(seq_id, task_args);
  // SubmitActorTask

  NotificationMessage msg(actor_id_, peer_actor_id_, queue_id_, seq_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  transport_->Send(std::move(buffer));
}

void ReaderQueue::CreateNotifyTask(uint64_t seq_id, std::vector<TaskArg> &task_args) {}

void ReaderQueue::OnData(QueueItem &item) {
  if (item.SeqId() != expect_seq_id_) {
    STREAMING_LOG(WARNING) << "OnData ignore seq_id: " << item.SeqId()
                           << " expect_seq_id_: " << expect_seq_id_;
    return;
  }

  last_recv_seq_id_ = item.SeqId();
  STREAMING_LOG(DEBUG) << "ReaderQueue::OnData seq_id: " << last_recv_seq_id_;

  Push(item);
  expect_seq_id_++;
}

}  // namespace streaming
}  // namespace ray
