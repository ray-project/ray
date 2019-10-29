#include "streaming_queue.h"
#include "config.h"
#include <chrono>
#include <thread>

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
    return NullQueueItem();
  }

  QueueItem item = buffer_queue_.front();
  return item;
}

QueueItem Queue::PopProcessed() {
  std::unique_lock<std::mutex> lock(mutex_);
  STREAMING_CHECK(buffer_queue_.size() != 0) << "WriterQueue Pop fail";

  if (watershed_iter_ == buffer_queue_.begin()) {
    return NullQueueItem();
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
    /// TODO:
    return QueueItem(QUEUE_INVALID_SEQ_ID, nullptr, 0, 0);
  }
}

QueueItem Queue::BackPending() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (std::next(watershed_iter_) == buffer_queue_.end()) {
    /// TODO
    return QueueItem(QUEUE_INVALID_SEQ_ID, nullptr, 0, 0);
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

    // pack
    // std::vector<TaskArg> task_args;
    // CreateSendMsgTask(item, task_args);
    // SubmitActorTask
    //
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

uint64_t WriterQueue::MockSend() {
  uint64_t count = 0;
  STREAMING_LOG(INFO) << "MockSend in";
  while (!IsPendingEmpty()) {
    STREAMING_LOG(INFO) << "MockSend";
    QueueItem item = PopPending();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    count++;
  }
  STREAMING_LOG(INFO) << "MockSend out";

  return count;
}

void WriterQueue::SendPullItem(QueueItem &item, uint64_t first_seq_id,
                               uint64_t last_seq_id) {
  PullDataMessage msg(actor_id_, peer_actor_id_, queue_id_, first_seq_id, item.SeqId(),
                      last_seq_id, item.Buffer(), item.IsRaw());
  STREAMING_CHECK(item.Buffer()->Data() != nullptr);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  transport_->Send(std::move(buffer));
}

void WriterQueue::CreateSendMsgTask(QueueItem &item, std::vector<TaskArg> &args) {}

void WriterQueue::OnNotify(std::shared_ptr<NotificationMessage> notify_msg) {
  STREAMING_LOG(INFO) << "OnNotify target seq_id: " << notify_msg->SeqId();
  min_consumed_id_ = notify_msg->SeqId();
  // TODO: Async notify user thread
}

// void WriterQueue::OnNotify(std::shared_ptr<NotificationMessage> notify_msg) {
//  uint64_t seq_id = notify_msg->SeqId();
//
//  QueueItem item = FrontProcessed();
//  STREAMING_LOG(INFO) << "OnNotify target seq_id: " << seq_id << " min seq_id: " <<
//  item.SeqId(); if (item.SeqId() == QUEUE_INVALID_SEQ_ID || seq_id < item.SeqId()) {
//    STREAMING_LOG(WARNING) << "OnNotify invalid seq_id: " << seq_id;
//    return;
//  }
//
//  while(item.SeqId() <= seq_id) {
//    STREAMING_LOG(INFO) << "OnNotify Pop " << item.SeqId();
//    PopProcessed();
//    item = FrontProcessed();
//  }
//}

int WriterQueue::SendPullItems(uint64_t target_seq_id, uint64_t first_seq_id, uint64_t last_seq_id) {
  STREAMING_LOG(INFO) << "SendPullItems : "
                      << " target_seq_id: " << target_seq_id
                      << " first_seq_id: " << first_seq_id
                      << " last_seq_id: " << last_seq_id;
  int count = 0;
  auto it = buffer_queue_.begin();
  std::advance(it, target_seq_id - first_seq_id);
  for (; it != watershed_iter_; it++) {
    STREAMING_LOG(INFO) << "OnPull send seq_id " << (*it).SeqId() << "(" << first_seq_id
                        << "/" << last_seq_id << ")"
                        << " to peer.";
    SendPullItem(*it, first_seq_id, last_seq_id);
    count++;
  }

  is_pulling_ = false;
  return count;
}

// TODO: What will happen if user push items while we are OnPull ?
// lock to sync OnPull and Push
std::shared_ptr<LocalMemoryBuffer> WriterQueue::OnPull(std::shared_ptr<PullRequestMessage> pull_msg, bool async, boost::asio::io_service &service) {
  std::unique_lock<std::mutex> lock(mutex_);
  uint64_t target_seq_id = pull_msg->SeqId();
  STREAMING_CHECK(peer_actor_id_ == pull_msg->ActorId())
      << peer_actor_id_ << " " << pull_msg->ActorId();

  auto it = buffer_queue_.begin();
  uint64_t first_seq_id = (*it).SeqId();
  // TODO: fix last_seq_id
  int distance = std::distance(it, watershed_iter_);
  uint64_t last_seq_id = first_seq_id + distance - 1;
  STREAMING_LOG(INFO) << "OnPull target_seq_id: " << target_seq_id
                      << " last_seq_id: " << last_seq_id
                      << " first_seq_id: " << first_seq_id
                      << " async: " << async;

  if (target_seq_id <= last_seq_id && target_seq_id >= first_seq_id) {
    if (async) {
      is_pulling_ = true;
      STREAMING_LOG(INFO) << "OnPull async, return";
      service.post(std::bind(&WriterQueue::SendPullItems, this, target_seq_id, first_seq_id, last_seq_id));
      PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                              pull_msg->QueueId(), queue::flatbuf::StreamingQueueError::OK);
      std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
      return std::move(buffer);
    }

    int count = SendPullItems(target_seq_id, first_seq_id, last_seq_id);
    STREAMING_LOG(INFO) << "OnPull total count: " << count;
    PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                            pull_msg->QueueId(), queue::flatbuf::StreamingQueueError::OK);
    std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

    transport_->Send(std::move(buffer));
    return nullptr;
  } else {
    STREAMING_LOG(WARNING) << "There is no valid data to pull.";
    PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                            pull_msg->QueueId(),
                            queue::flatbuf::StreamingQueueError::NO_VALID_DATA_TO_PULL);
    std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

    if (async) {
      return std::move(buffer);
    } else {
      transport_->Send(std::move(buffer));
      return nullptr;
    }
  }
}

void ReaderQueue::OnConsumed(uint64_t seq_id) {
  STREAMING_LOG(INFO) << "OnConsumed: " << seq_id;
  //  if (seq_id < min_consumed_id_) {
  //    STREAMING_LOG(WARNING) << "OnConsumed nothing to consume: " << seq_id;
  //    return;
  //  }
  //  // delete item in local, and then notify upstream
  //  for (uint64_t id = min_consumed_id_; id <= seq_id; id++) {
  //    QueueItem item = PopProcessed();
  //    STREAMING_CHECK(item.SeqId() == id) << item.SeqId() << " " << id;
  //    STREAMING_LOG(INFO) << "OnConsumed Pop: " << seq_id;
  //    /// TODO: release buffer
  //  }
  //
  //  min_consumed_id_ = seq_id + 1;

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

/// TODO: add timeout
bool ReaderQueue::PullPeerSync(uint64_t seq_id) {
  PullRequestMessage msg(actor_id_, peer_actor_id_, queue_id_, seq_id, /*sync*/false);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  promise_for_pull_.reset(new PromiseWrapper());
  transport_->Send(std::move(buffer));

  Status st = promise_for_pull_->WaitFor(PULL_UPPSTREAM_DATA_TIMEOUT_MS);
  promise_for_pull_ = nullptr;
  return st.ok();
}

void ReaderQueue::OnData(QueueItem &item) {
  if (item.SeqId() != expect_seq_id_) {
    STREAMING_LOG(WARNING) << "OnData ignore seq_id: " << item.SeqId()
                           << " expect_seq_id_: " << expect_seq_id_;
    return;
  }

  last_recv_seq_id_ = item.SeqId();
  /// TODO: Should not parse
  last_recv_msg_id_ = item.MaxMsgId();
  STREAMING_LOG(DEBUG) << "ReaderQueue::OnData seq_id: " << last_recv_seq_id_
                       << " msg_id: " << last_recv_msg_id_;
  Push(item);
  expect_seq_id_++;
}

void ReaderQueue::OnPullData(std::shared_ptr<PullDataMessage> msg) {
  // TODO: fix timestamp parameter
  STREAMING_LOG(INFO) << "OnPullData recv seq_id " << msg->SeqId() << "("
                      << msg->FirstSeqId() << "/" << msg->LastSeqId() << ")";
  QueueItem item(msg->SeqId(), msg->Buffer(), 0, msg->IsRaw());
  STREAMING_CHECK(msg->Buffer()->Data() != nullptr);

  if (item.SeqId() != expect_seq_id_) {
    STREAMING_LOG(WARNING) << "OnPullData ignore seq_id: " << item.SeqId()
                           << " expect_seq_id_: " << expect_seq_id_;
    return;
  }

  Push(item);
  expect_seq_id_++;

  STREAMING_CHECK(msg->SeqId() >= msg->FirstSeqId() && msg->SeqId() <= msg->LastSeqId())
      << "(" << msg->FirstSeqId() << "/" << msg->SeqId() << "/" << msg->LastSeqId()
      << ")";
  if (msg->SeqId() == msg->LastSeqId()) {
    STREAMING_LOG(INFO) << "Pull DATA Done";
  }
}

void ReaderQueue::OnPullResponse(std::shared_ptr<PullResponseMessage> msg) {
  if (nullptr == promise_for_pull_) return;

  if (msg->Error() == queue::flatbuf::StreamingQueueError::OK) {
    promise_for_pull_->Notify(Status::OK());
  } else {
    promise_for_pull_->Notify(Status::Invalid("No invalid data."));
  }
}

}  // namespace streaming
}  // namespace ray
