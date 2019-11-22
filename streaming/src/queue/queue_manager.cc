#include "queue_manager.h"
#include "utils.h"
#include "config.h"

namespace ray {
namespace streaming {

constexpr uint64_t COMMON_SYNC_CALL_TIMEOUTT_MS = 5*1000;

std::shared_ptr<QueueManager> QueueManager::queue_manager_ = nullptr;

QueueWriter::~QueueWriter() {
  manager_->ReleaseAllUpQueues();
}

void QueueWriter::CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                              const ActorID &peer_actor_id, uint64_t size) {
  STREAMING_LOG(INFO) << "QueueWriter::CreateQueue";
  auto queue = manager_->CreateUpstreamQueue(queue_id, actor_id, peer_actor_id, size);
  STREAMING_CHECK(queue != nullptr);

  manager_->UpdateUpActor(queue_id, actor_id);
  manager_->UpdateDownActor(queue_id, peer_actor_id);

  std::vector<ObjectID> queue_ids, failed_queues;
  queue_ids.push_back(queue_id);
  WaitQueues(queue_ids, 10*1000, failed_queues);
}

void QueueWriter::SetQueueEvictionLimit(const ObjectID &queue_id, uint64_t limit) {
  std::shared_ptr<WriterQueue> queue = manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  queue->SetQueueEvictionLimit(limit);
}

void QueueWriter::WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                             std::vector<ObjectID> &failed_queues) {
  STREAMING_LOG(INFO) << "QueueWriter::WaitQueues timeout_ms: " << timeout_ms;
  manager_->WaitQueues(queue_ids, timeout_ms, failed_queues, DOWNSTREAM);
}

uint64_t QueueWriter::GetMinConsumedSeqID(const ObjectID &queue_id) {
  std::shared_ptr<WriterQueue> queue = manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  return queue->GetMinConsumedSeqID();
}

Status QueueWriter::PushSync(const ObjectID &queue_id, uint64_t seq_id, uint8_t *data,
                             uint32_t data_size, uint64_t timestamp, bool raw) {
  STREAMING_LOG(INFO) << "QueueWriter::PushSync:"
                       << " qid: " << queue_id << " seq_id: " << seq_id
                       << " data_size: " << data_size << " raw: " << raw;
  std::shared_ptr<WriterQueue> queue = manager_->GetUpQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);

  Status status = queue->Push(seq_id, data, data_size, timestamp, raw);
  if (status.IsOutOfMemory()) {
    Status st = queue->TryEvictItems();
    if (!st.ok()) {
      STREAMING_LOG(INFO) << "Evict fail.";
      return st;
    }

    st = queue->Push(seq_id, data, data_size, timestamp, raw);
    STREAMING_LOG(INFO) << "After evict PushSync: " << st.ok();
    return st;
  }

  queue->Send();
  return status;
}

QueueReader::~QueueReader() {
  manager_->ReleaseAllDownQueues();
}

bool QueueReader::CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                              const ActorID &peer_actor_id, uint64_t start_seq_id) {
  STREAMING_LOG(INFO) << "Create ReaderQueue " << queue_id
                      << " pull from start_seq_id: " << start_seq_id;
  manager_->CreateDownstreamQueue(queue_id, actor_id, peer_actor_id);
  return true;
}

void QueueReader::GetQueueItem(const ObjectID &queue_id, uint8_t *&data,
                               uint32_t &data_size, uint64_t &seq_id,
                               uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "GetQueueItem qid: " << queue_id;
  auto queue = manager_->GetDownQueue(queue_id);
  QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
  if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
    STREAMING_LOG(INFO) << "GetQueueItem timeout.";
    data = nullptr;
    data_size = 0;
    seq_id = QUEUE_INVALID_SEQ_ID;
    return;
  }

  data = item.Buffer()->Data();
  seq_id = item.SeqId();
  data_size = item.Buffer()->Size();

  STREAMING_LOG(DEBUG) << "GetQueueItem qid: " << queue_id
                       << " seq_id: " << seq_id
                       << " msg_id: " << item.MaxMsgId()
                       << " data_size: " << data_size;
}

void QueueReader::NotifyConsumedItem(const ObjectID &queue_id, uint64_t seq_id) {
  STREAMING_LOG(DEBUG) << "QueueReader::NotifyConsumedItem";
  auto queue = manager_->GetDownQueue(queue_id);
  queue->OnConsumed(seq_id);
}

void QueueManager::Init() {
  queue_thread_ = std::thread(&QueueManager::QueueThreadCallback, this);
}

std::shared_ptr<WriterQueue> QueueManager::CreateUpstreamQueue(const ObjectID &queue_id,
                                                         const ActorID &actor_id,
                                                         const ActorID &peer_actor_id,
                                                         uint64_t size) {
  STREAMING_LOG(INFO) << "CreateUpstreamQueue: " << queue_id
                      << " " << actor_id << "->" << peer_actor_id;
  std::shared_ptr<WriterQueue> queue = GetUpQueue(queue_id);
  if (queue != nullptr) {
    STREAMING_LOG(WARNING) << "Duplicate to create up queue." << queue_id;
    return queue;
  }

  queue = std::unique_ptr<streaming::WriterQueue>(new streaming::WriterQueue(
            queue_id, actor_id, peer_actor_id, size, GetOutTransport(queue_id)));
  upstream_queues_[queue_id] = queue;

  return queue;
}

bool QueueManager::UpstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetUpQueue(queue_id);
}

bool QueueManager::DownstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetDownQueue(queue_id);
}

std::shared_ptr<ReaderQueue> QueueManager::CreateDownstreamQueue(const ObjectID &queue_id,
                                                           const ActorID &actor_id,
                                                           const ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "CreateDownstreamQueue: " << queue_id
                      << " " << peer_actor_id << "->" << actor_id;
  auto it = downstream_queues_.find(queue_id);
  if (it != downstream_queues_.end()) {
    STREAMING_LOG(WARNING) << "Duplicate to create down queue!!!! " << queue_id;
    return it->second;
  }

  std::shared_ptr<streaming::ReaderQueue> queue =
      std::unique_ptr<streaming::ReaderQueue>(new streaming::ReaderQueue(
          queue_id, actor_id, peer_actor_id, GetOutTransport(queue_id)));
  downstream_queues_[queue_id] = queue;
  return queue;
}

std::shared_ptr<streaming::WriterQueue> QueueManager::GetUpQueue(
    const ObjectID &queue_id) {
  auto it = upstream_queues_.find(queue_id);
  if (it == upstream_queues_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<streaming::ReaderQueue> QueueManager::GetDownQueue(
    const ObjectID &queue_id) {
  auto it = downstream_queues_.find(queue_id);
  if (it == downstream_queues_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<Message> QueueManager::ParseMessage(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  uint8_t *bytes = buffer->Data();
  uint8_t *p_cur = bytes;
  uint32_t *magic_num = (uint32_t *)p_cur;
  STREAMING_CHECK(*magic_num == Message::MagicNum) << *magic_num << " " << Message::MagicNum;

  p_cur += sizeof(Message::MagicNum);
  queue::protobuf::StreamingQueueMessageType *type = (queue::protobuf::StreamingQueueMessageType *)p_cur;

  std::shared_ptr<Message> message = nullptr;
  switch (*type) {
  case queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType:
    message = NotificationMessage::FromBytes(bytes);
    break;
  case queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType:
    message = DataMessage::FromBytes(bytes);
    break;
  case queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType:
    message = CheckMessage::FromBytes(bytes);
    break;
  case queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType:
    message = CheckRspMessage::FromBytes(bytes);
    break;
  default:
    STREAMING_CHECK(false) << "nonsupport message type: "
                           << queue::protobuf::StreamingQueueMessageType_Name(*type);
    break;
  }

  return message;
}

void QueueManager::DispatchMessageInternal(
    std::shared_ptr<LocalMemoryBuffer> buffer,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::shared_ptr<Message> msg = ParseMessage(buffer);
  STREAMING_LOG(DEBUG) << "QueueManager::DispatchMessageInternal: "
                       << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
                       << " peer actorid: " << msg->PeerActorId()
                       << " type: " << queue::protobuf::StreamingQueueMessageType_Name(msg->Type());

  if (msg->Type() == queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType) {
    auto queue = upstream_queues_.find(msg->QueueId());
    if (queue == upstream_queues_.end()) {
        std::shared_ptr<NotificationMessage> notify_msg = 
          std::dynamic_pointer_cast<NotificationMessage>(msg);
      STREAMING_LOG(WARNING) << "Can not find queue for " << queue::protobuf::StreamingQueueMessageType_Name(msg->Type())
                             << ", maybe queue has been destroyed, ignore it."
                             << " seq id: " << notify_msg->SeqId();
      return;
    }
    std::shared_ptr<NotificationMessage> notify_msg =
        std::dynamic_pointer_cast<NotificationMessage>(msg);

    queue->second->OnNotify(notify_msg);
  } else if (msg->Type() == queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType) {
    auto queue = downstream_queues_.find(msg->QueueId());
    if (queue == downstream_queues_.end()) {
      std::shared_ptr<DataMessage> data_msg = std::dynamic_pointer_cast<DataMessage>(msg);
      STREAMING_LOG(WARNING) << "Can not find queue for " << queue::protobuf::StreamingQueueMessageType_Name(msg->Type())
                             << ", maybe queue has been destroyed, ignore it."
                             << " seq id: " << data_msg->SeqId();
      return;
    }
    std::shared_ptr<DataMessage> data_msg = std::dynamic_pointer_cast<DataMessage>(msg);

    QueueItem item(data_msg);
    queue->second->OnData(item);
  } else if (msg->Type() == queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType) {
    std::shared_ptr<LocalMemoryBuffer> check_result =
        this->OnCheckQueue(std::dynamic_pointer_cast<CheckMessage>(msg));
    if (callback != nullptr) {
      callback(check_result);
    }
  } else if (msg->Type() == queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType) {
    this->OnCheckQueueRsp(std::dynamic_pointer_cast<CheckRspMessage>(msg));
    STREAMING_CHECK(false) << "Should not receive StreamingQueueCheckRspMsg";
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::protobuf::StreamingQueueMessageType_Name(msg->Type());
  }
}

// Message will be handled in io_service queue thread.
void QueueManager::DispatchMessage(std::shared_ptr<LocalMemoryBuffer> buffer) {
  queue_service_.post(
      boost::bind(&QueueManager::DispatchMessageInternal, this, buffer, nullptr));
}

std::shared_ptr<LocalMemoryBuffer> QueueManager::DispatchMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  std::shared_ptr<LocalMemoryBuffer> result = nullptr;
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  queue_service_.post(
      boost::bind(&QueueManager::DispatchMessageInternal, this, buffer,
                  [&promise, &result](std::shared_ptr<LocalMemoryBuffer> rst) {
                    result = rst;
                    promise->Notify(ray::Status::OK());
                  }));
  Status st = promise->Wait();
  STREAMING_CHECK(st.ok());

  return result;
}

bool QueueManager::CheckQueue(const ObjectID &queue_id, QueueType type) {
  auto it = actors_.find(queue_id);
  STREAMING_CHECK(it != actors_.end());
  ActorID peer_actor_id;
  if (UPSTREAM == type) {
    peer_actor_id = it->second.first;
  } else {
    peer_actor_id = it->second.second;
  }
  STREAMING_LOG(INFO) << "CheckQueue queue_id: " << queue_id
                      << " peer_actor_id: " << peer_actor_id;

  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  check_queue_requests_[queue_id] =
      CheckQueueRequest(peer_actor_id, queue_id, [promise](bool rst) {
        if (rst) {
          promise->Notify(Status::OK());
        } else {
          promise->Notify(Status::Invalid("Queue Not Ready"));
        }
      });

  CheckMessage msg(actor_id_, peer_actor_id, queue_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  transport_it->Send(std::move(buffer));

  Status st = promise->WaitFor(500);
  return st.ok();
}

bool QueueManager::CheckQueueSync(const ObjectID &queue_id, QueueType type) {
  auto it = actors_.find(queue_id);
  STREAMING_CHECK(it != actors_.end());
  ActorID peer_actor_id;
  if (UPSTREAM == type) {
    peer_actor_id = it->second.first;
  } else {
    peer_actor_id = it->second.second;
  }
  STREAMING_LOG(INFO) << "CheckQueueSync queue_id: " << queue_id
                      << " peer_actor_id: " << peer_actor_id;

  CheckMessage msg(actor_id_, peer_actor_id, queue_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer =
      transport_it->SendForResultWithRetry(std::move(buffer), 10, COMMON_SYNC_CALL_TIMEOUTT_MS);
  if (result_buffer == nullptr) {
    return false;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(result_msg->Type() ==
                  queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType);
  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::dynamic_pointer_cast<CheckRspMessage>(result_msg);
  STREAMING_LOG(INFO) << "CheckQueueSync return queue_id: " << check_rsp_msg->QueueId();
  STREAMING_CHECK(check_rsp_msg->PeerActorId() == actor_id_);

  return queue::protobuf::StreamingQueueError::OK == check_rsp_msg->Error();
}

void QueueManager::WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                              std::vector<ObjectID> &failed_queues, QueueType type) {
  failed_queues.insert(failed_queues.begin(), queue_ids.begin(), queue_ids.end());
  uint64_t start_time_us = current_time_ms();
  uint64_t current_time_us = start_time_us;
  while (!failed_queues.empty() && current_time_us < start_time_us + timeout_ms * 1000) {
    for (auto it = failed_queues.begin(); it != failed_queues.end();) {
      if (CheckQueueSync(*it, type)) {
        STREAMING_LOG(INFO) << "Check queue: " << *it << " return, ready.";
        it = failed_queues.erase(it);
      } else {
        STREAMING_LOG(INFO) << "Check queue: " << *it << " return, not ready.";
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        it++;
      }
    }
    current_time_us = current_time_ms();
  }
}

void QueueManager::UpdateUpActor(const ObjectID &queue_id, const ActorID &actor_id) {
  std::pair<ActorID, ActorID> actor_pair = std::make_pair(actor_id, ActorID::Nil());
  auto it = actors_.find(queue_id);
  if (it != actors_.end()) {
    actor_pair.second = it->second.second;
  }

  actors_[queue_id] = actor_pair;
}

void QueueManager::UpdateDownActor(const ObjectID &queue_id, const ActorID &actor_id) {
  std::pair<ActorID, ActorID> actor_pair = std::make_pair(ActorID::Nil(), actor_id);
  auto it = actors_.find(queue_id);
  if (it != actors_.end()) {
    actor_pair.first = it->second.first;
  }

  actors_[queue_id] = actor_pair;
}

void QueueManager::AddOutTransport(const ObjectID &queue_id,
                                   std::shared_ptr<Transport> transport) {
  STREAMING_LOG(INFO) << "AddOutTransport for queue: " << queue_id;
  out_transports_.emplace(queue_id, transport);
}

std::shared_ptr<Transport> QueueManager::GetOutTransport(const ObjectID &queue_id) {
  auto it = out_transports_.find(queue_id);
  if (it == out_transports_.end()) return nullptr;

  return it->second;
}

void QueueManager::Stop() {
  STREAMING_LOG(INFO) << "QueueManager Stop.";
  queue_service_.stop();
  if (queue_thread_.joinable()) {
    queue_thread_.join();
  }
}

std::shared_ptr<LocalMemoryBuffer> QueueManager::OnCheckQueue(
    std::shared_ptr<CheckMessage> check_msg) {
  queue::protobuf::StreamingQueueError err_code = queue::protobuf::StreamingQueueError::OK;
  auto up_queue = upstream_queues_.find(check_msg->QueueId());
  if (up_queue == upstream_queues_.end()) {
    auto down_queue = downstream_queues_.find(check_msg->QueueId());
    if (down_queue == downstream_queues_.end()) {
      STREAMING_LOG(WARNING) << "OnCheckQueue " << check_msg->QueueId() << " not found.";
      err_code = queue::protobuf::StreamingQueueError::QUEUE_NOT_EXIST;
    }
  }

  CheckRspMessage msg(check_msg->PeerActorId(), check_msg->ActorId(),
                      check_msg->QueueId(), err_code);
  std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  return buffer;
}

void QueueManager::OnCheckQueueRsp(std::shared_ptr<CheckRspMessage> check_rsp_msg) {
  STREAMING_LOG(INFO) << "OnCheckQueueRsp: " << check_rsp_msg->QueueId();
  STREAMING_CHECK(check_rsp_msg->PeerActorId() == actor_id_);
  auto it = check_queue_requests_.find(check_rsp_msg->QueueId());
  STREAMING_CHECK(it != check_queue_requests_.end());

  it->second.callback_(queue::protobuf::StreamingQueueError::OK == check_rsp_msg->Error());
}

void QueueManager::ReleaseAllUpQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllUpQueues";
  for (auto &it : upstream_queues_) {
    actors_.erase(it.first);
    out_transports_.erase(it.first);
  }
  upstream_queues_.clear();
}

void QueueManager::ReleaseAllDownQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues size: " << downstream_queues_.size();
  for (auto &it : downstream_queues_) {
    actors_.erase(it.first);
    out_transports_.erase(it.first);
  }
  downstream_queues_.clear();
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues done: " << downstream_queues_.size();
}

}  // namespace streaming
}  // namespace ray