#include "queue_manager.h"
#include "utils.h"
#include "config.h"

namespace ray {
namespace streaming {

std::shared_ptr<QueueManager> QueueManager::queue_manager_ = nullptr;

QueueWriter::~QueueWriter() {
  manager_->ReleaseAllUpQueues();
}

void QueueWriter::CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                              const ActorID &peer_actor_id, uint64_t size, bool clear) {
  STREAMING_LOG(INFO) << "QueueWriter::CreateQueue";
  auto queue = manager_->CreateUpQueue(queue_id, actor_id, peer_actor_id, size, clear);
  STREAMING_CHECK(queue != nullptr);

  manager_->UpdateUpActor(queue_id, actor_id);
  manager_->UpdateDownActor(queue_id, peer_actor_id);

  if (!clear) {
    uint64_t last_queue_msg_id, last_queue_seq_id;
    manager_->GetPeerLastMsgId(queue_id, last_queue_msg_id, last_queue_seq_id);
    queue->SetPeerLastIds(last_queue_msg_id, last_queue_seq_id);
  }
  STREAMING_LOG(INFO) << "QueueWriter::CreateQueue done";
}

bool QueueWriter::IsQueueExist(const ObjectID &queue_id) {
  return nullptr != manager_->GetUpQueue(queue_id);
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
  STREAMING_LOG(DEBUG) << "QueueWriter::PushSync:"
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

/// Just return invalid seq_id because we do not pull data from downstream
uint64_t QueueWriter::GetLastQueueItem(const ObjectID &queue_id) {
  return QUEUE_INVALID_SEQ_ID;
}

void QueueWriter::GetPeerLastMsgId(const ObjectID &queue_id, uint64_t &last_queue_msg_id, 
                                       uint64_t &last_queue_seq_id) {
  auto queue = manager_->GetUpQueue(queue_id);
  last_queue_msg_id = queue->GetPeerLastMsgId();
  last_queue_seq_id = queue->GetPeerLastSeqId();
}

QueueReader::~QueueReader() {
  manager_->ReleaseAllDownQueues();
}

bool QueueReader::CreateQueue(const ObjectID &queue_id, const ActorID &actor_id,
                              const ActorID &peer_actor_id, uint64_t start_seq_id) {
  STREAMING_LOG(INFO) << "Create ReaderQueue " << queue_id
                      << " pull from start_seq_id: " << start_seq_id;
  auto queue = manager_->CreateDownQueue(queue_id, actor_id, peer_actor_id);
  STREAMING_CHECK(queue != nullptr);

  std::vector<ObjectID> queue_ids;
  queue_ids.push_back(queue_id);

  if (start_seq_id != 1) {
    STREAMING_LOG(INFO) << "Need pull data from upstream, start_seq_id: " << start_seq_id;
    // STREAMING_LOG(INFO) << "PullPeerSync with timeout";
    // return queue->PullPeerSync(start_seq_id);
    STREAMING_LOG(INFO) << "PullPeerAsync ";
    return manager_->PullPeerAsync(queue_id, actor_id, peer_actor_id, start_seq_id);
    // return true;
  } else {
    STREAMING_LOG(INFO) << "No need pull data from upstream";
    return true;
  }
}

void QueueReader::GetQueueItem(const ObjectID &queue_id, uint8_t *&data,
                               uint32_t &data_size, uint64_t &seq_id,
                               uint64_t timeout_ms) {
  STREAMING_LOG(DEBUG) << "GetQueueItem qid: " << queue_id;
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

/// TODO: Maintain a seq_id represent last_seq_id ?
uint64_t QueueReader::GetLastSeqID(const ObjectID &queue_id) {
  auto queue = manager_->GetDownQueue(queue_id);

  return queue->GetLastRecvSeqId();
}

void QueueReader::WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                             std::vector<ObjectID> &failed_queues) {
  manager_->WaitQueues(queue_ids, timeout_ms, failed_queues, UPSTREAM);
}

void QueueManager::Init() {
  /// TODO: do not start corresponding thread when queue not exist.
  boost::asio::io_service service;
  queue_thread_ = std::thread(&QueueManager::QueueThreadCallback, this);
}

std::shared_ptr<WriterQueue> QueueManager::CreateUpQueue(const ObjectID &queue_id,
                                                         const ActorID &actor_id,
                                                         const ActorID &peer_actor_id,
                                                         uint64_t size, bool clear) {
  STREAMING_LOG(INFO) << "CreateUpQueue: " << queue_id
                      << " " << actor_id << "->" << peer_actor_id;
  auto it = upstream_queues_.find(queue_id);
  // RAY_CHECK(it == upstream_queues_.end()) << "duplicate to create queue: " << queue_id;
  if (it != upstream_queues_.end()) {
    STREAMING_LOG(WARNING) << "Duplicate to create up queue!!!! " << queue_id;
    return it->second;
  }

  std::shared_ptr<streaming::WriterQueue> queue =
      std::unique_ptr<streaming::WriterQueue>(new streaming::WriterQueue(
          queue_id, actor_id, peer_actor_id, size, clear, GetOutTransport(queue_id)));
  upstream_queues_[queue_id] = queue;

  upstream_state_ = StreamingQueueState::Running;
  return queue;
}

bool QueueManager::IsUpQueueExist(const ObjectID &queue_id) {
  auto it = upstream_queues_.find(queue_id);
  return it != upstream_queues_.end();
}

bool QueueManager::IsDownQueueExist(const ObjectID &queue_id) {
  auto it = downstream_queues_.find(queue_id);
  return it != downstream_queues_.end();
}

std::shared_ptr<ReaderQueue> QueueManager::CreateDownQueue(const ObjectID &queue_id,
                                                           const ActorID &actor_id,
                                                           const ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "CreateDownQueue: " << queue_id
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

  downstream_state_ = StreamingQueueState::Running;

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
  STREAMING_CHECK(*magic_num == Message::MagicNum);

  p_cur += sizeof(Message::MagicNum);
  queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;

  std::shared_ptr<Message> message = nullptr;
  switch (*type) {
  case queue::flatbuf::MessageType::StreamingQueueNotificationMsg:
    message = NotificationMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueDataMsg:
    message = DataMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueCheckMsg:
    message = CheckMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueuePullRequestMsg:
    message = PullRequestMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueuePullResponseMsg:
    message = PullResponseMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueuePullDataMsg:
    message = PullDataMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueCheckRspMsg:
    message = CheckRspMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetLastMsgId:
    message= GetLastMsgIdMessage::FromBytes(bytes);
    break;
  case queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp:
    message= GetLastMsgIdRspMessage::FromBytes(bytes);
    break;
  default:
    STREAMING_CHECK(false) << "nonsupport message type: "
                           << queue::flatbuf::EnumNameMessageType(*type);
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
                       << " type: " << queue::flatbuf::EnumNameMessageType(msg->Type());

  if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueNotificationMsg) {
    if (upstream_state_ != StreamingQueueState::Running) {
      STREAMING_LOG(WARNING) << "up queues are not running.";
      return;
    }
    auto queue = upstream_queues_.find(msg->QueueId());
    // STREAMING_CHECK(queue != upstream_queues_.end());
    if (queue == upstream_queues_.end()) {
        std::shared_ptr<NotificationMessage> notify_msg = 
          std::dynamic_pointer_cast<NotificationMessage>(msg);
      STREAMING_LOG(WARNING) << "Can not find queue for " << queue::flatbuf::EnumNameMessageType(msg->Type())
                             << ", maybe queue has been destroyed, ignore it."
                             << " seq id: " << notify_msg->SeqId();
      return;
    }
    std::shared_ptr<NotificationMessage> notify_msg =
        std::dynamic_pointer_cast<NotificationMessage>(msg);

    queue->second->OnNotify(notify_msg);
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueDataMsg) {
    if (downstream_state_ != StreamingQueueState::Running) {
      STREAMING_LOG(WARNING) << "down queues are not running.";
      return;
    }
    auto queue = downstream_queues_.find(msg->QueueId());
    // STREAMING_CHECK(queue != downstream_queues_.end());
    if (queue == downstream_queues_.end()) {
      std::shared_ptr<DataMessage> data_msg = std::dynamic_pointer_cast<DataMessage>(msg);
      STREAMING_LOG(WARNING) << "Can not find queue for " << queue::flatbuf::EnumNameMessageType(msg->Type())
                             << ", maybe queue has been destroyed, ignore it."
                             << " seq id: " << data_msg->SeqId();
      return;
    }
    std::shared_ptr<DataMessage> data_msg = std::dynamic_pointer_cast<DataMessage>(msg);

    QueueItem item(data_msg);
    queue->second->OnData(item);
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueuePullRequestMsg) {
    if (upstream_state_ != StreamingQueueState::Running) {
      STREAMING_LOG(WARNING) << "up queues are not running.";
      return;
    }
    auto queue = upstream_queues_.find(msg->QueueId());
    STREAMING_CHECK(queue != upstream_queues_.end());
    std::shared_ptr<PullRequestMessage> pull_msg =
        std::dynamic_pointer_cast<PullRequestMessage>(msg);

    if (pull_msg->IsAsync() && callback != nullptr) {
      callback(queue->second->OnPull(pull_msg, true, queue_service_));
    } else {
      queue->second->OnPull(pull_msg, false, queue_service_);
    }
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueuePullDataMsg) {
    if (downstream_state_ != StreamingQueueState::Running) {
      STREAMING_LOG(WARNING) << "down queues are not running.";
      return;
    }
    auto queue = downstream_queues_.find(msg->QueueId());
    STREAMING_CHECK(queue != downstream_queues_.end());
    std::shared_ptr<PullDataMessage> pull_data_msg =
        std::dynamic_pointer_cast<PullDataMessage>(msg);

    queue->second->OnPullData(pull_data_msg);
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueuePullResponseMsg) {
    if (downstream_state_ != StreamingQueueState::Running) {
      STREAMING_LOG(WARNING) << "down queues are not running.";
      return;
    }
    auto queue = downstream_queues_.find(msg->QueueId());
    STREAMING_CHECK(queue != downstream_queues_.end());
    std::shared_ptr<PullResponseMessage> pull_rsp_msg =
        std::dynamic_pointer_cast<PullResponseMessage>(msg);

    queue->second->OnPullResponse(pull_rsp_msg);
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueCheckMsg) {
    std::shared_ptr<LocalMemoryBuffer> check_result =
        this->OnCheckQueue(std::dynamic_pointer_cast<CheckMessage>(msg));
    if (callback != nullptr) {
      callback(check_result);
    }
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueCheckRspMsg) {
    this->OnCheckQueueRsp(std::dynamic_pointer_cast<CheckRspMessage>(msg));
    STREAMING_CHECK(false) << "Should not receive StreamingQueueCheckRspMsg";
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueGetLastMsgId) {
    std::shared_ptr<LocalMemoryBuffer> last_msg_result =
        this->OnGetLastMsgId(std::dynamic_pointer_cast<GetLastMsgIdMessage>(msg));
    if (callback != nullptr) {
      callback(last_msg_result);
    }
  } else if (msg->Type() == queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp) {
    STREAMING_CHECK(false) << "Should not receive StreamingQueueGetLastMsgIdRsp";
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::flatbuf::EnumNameMessageType(msg->Type());
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

void QueueManager::PullPeer(const ObjectID &queue_id, uint64_t start_seq_id) {
  auto it = downstream_queues_.find(queue_id);
  RAY_CHECK(it != downstream_queues_.end());

  it->second->SetExpectSeqId(start_seq_id);
  it->second->PullPeerSync(start_seq_id);
}

void QueueManager::SetMinConsumedSeqId(const ObjectID &queue_id, uint64_t seq_id) {
  auto it = downstream_queues_.find(queue_id);
  RAY_CHECK(it != downstream_queues_.end());

  return it->second->SetMinConsumedSeqId(seq_id);
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
                  queue::flatbuf::MessageType::StreamingQueueCheckRspMsg);
  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::dynamic_pointer_cast<CheckRspMessage>(result_msg);
  STREAMING_LOG(INFO) << "CheckQueueSync return queue_id: " << check_rsp_msg->QueueId();
  STREAMING_CHECK(check_rsp_msg->PeerActorId() == actor_id_);

  return queue::flatbuf::StreamingQueueError::OK == check_rsp_msg->Error();
}

void QueueManager::WaitQueues(const std::vector<ObjectID> &queue_ids, int64_t timeout_ms,
                              std::vector<ObjectID> &failed_queues, QueueType type) {
  failed_queues.insert(failed_queues.begin(), queue_ids.begin(), queue_ids.end());
  uint64_t start_time_us = current_sys_time_us();
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
    current_time_us = current_sys_time_us();
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

void QueueManager::SetInTransport(std::shared_ptr<Transport> transport) {
  in_transport_ = transport;
}

std::shared_ptr<Transport> QueueManager::GetOutTransport(const ObjectID &queue_id) {
  auto it = out_transports_.find(queue_id);
  if (it == out_transports_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<Transport> QueueManager::GetInTransport() { return in_transport_; }

void QueueManager::Stop() {
  STREAMING_LOG(INFO) << "QueueManager Stop.";
  queue_service_.stop();
  if (queue_thread_.joinable()) {
    queue_thread_.join();
  }
}

std::shared_ptr<LocalMemoryBuffer> QueueManager::OnCheckQueue(
    std::shared_ptr<CheckMessage> check_msg) {
  queue::flatbuf::StreamingQueueError err_code = queue::flatbuf::StreamingQueueError::OK;
  auto up_queue = upstream_queues_.find(check_msg->QueueId());
  if (up_queue == upstream_queues_.end()) {
    auto down_queue = downstream_queues_.find(check_msg->QueueId());
    if (down_queue == downstream_queues_.end()) {
      STREAMING_LOG(WARNING) << "OnCheckQueue " << check_msg->QueueId() << " not found.";
      err_code = queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST;
    }
  }

  CheckRspMessage msg(check_msg->PeerActorId(), check_msg->ActorId(),
                      check_msg->QueueId(), err_code);
  std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  STREAMING_LOG(INFO) << "OnCheckQueue actor_id: " << check_msg->ActorId();

  return buffer;
}

void QueueManager::OnCheckQueueRsp(std::shared_ptr<CheckRspMessage> check_rsp_msg) {
  STREAMING_LOG(INFO) << "OnCheckQueueRsp: " << check_rsp_msg->QueueId();
  STREAMING_CHECK(check_rsp_msg->PeerActorId() == actor_id_);
  auto it = check_queue_requests_.find(check_rsp_msg->QueueId());
  STREAMING_CHECK(it != check_queue_requests_.end());

  it->second.callback_(queue::flatbuf::StreamingQueueError::OK == check_rsp_msg->Error());
}

void QueueManager::GetPeerLastMsgId(const ObjectID &queue_id, uint64_t &last_queue_msg_id,
                                        uint64_t &last_queue_seq_id) {
  STREAMING_LOG(INFO) << "GetPeerLastMsgId qid: " << queue_id;
  auto it = actors_.find(queue_id);
  STREAMING_CHECK(it != actors_.end());
  ActorID &peer_actor_id = it->second.second;

  GetLastMsgIdMessage msg(actor_id_, peer_actor_id, queue_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer =
      transport_it->SendForResultWithRetry(std::move(buffer), 10, COMMON_SYNC_CALL_TIMEOUTT_MS);
  if (result_buffer == nullptr) {
    last_queue_msg_id = 0;
    last_queue_seq_id = QUEUE_INVALID_SEQ_ID;
    return;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(result_msg->Type() ==
                  queue::flatbuf::MessageType::StreamingQueueGetLastMsgIdRsp);
  std::shared_ptr<GetLastMsgIdRspMessage> get_rsp_msg =
      std::dynamic_pointer_cast<GetLastMsgIdRspMessage>(result_msg);
  STREAMING_LOG(INFO) << "GetPeerLastMsgId return queue_id: " << get_rsp_msg->QueueId()
                      << " error: " << queue::flatbuf::EnumNameStreamingQueueError(get_rsp_msg->Error());
  STREAMING_CHECK(get_rsp_msg->PeerActorId() == actor_id_);

  if (queue::flatbuf::StreamingQueueError::OK != get_rsp_msg->Error()) {
    // return 0 because Producer::FetchLastMessageIdFromQueue return 0 defaultly.
    last_queue_msg_id = 0;
    // set to QUEUE_INVALID_SEQ_ID because Producer::FetchLastMessageIdFromQueue 'queue_last_seq_id == static_cast<uint64_t>(-1)'
    last_queue_seq_id = QUEUE_INVALID_SEQ_ID;
    return;
  }

  last_queue_seq_id = get_rsp_msg->SeqId();
  last_queue_msg_id = get_rsp_msg->MsgId();
}

std::shared_ptr<LocalMemoryBuffer> QueueManager::OnGetLastMsgId(
    std::shared_ptr<GetLastMsgIdMessage> get_msg) {
  STREAMING_LOG(WARNING) << "OnGetLastMsgId " << get_msg->QueueId();
  auto down_queue = downstream_queues_.find(get_msg->QueueId());
  if (down_queue == downstream_queues_.end()) {
    STREAMING_LOG(WARNING) << "OnGetLastMsgId " << get_msg->QueueId() << " not found.";
    queue::flatbuf::StreamingQueueError err_code = queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST;
    GetLastMsgIdRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                               get_msg->QueueId(), QUEUE_INVALID_SEQ_ID,
                               0, err_code);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    return buffer;
  } else {
    queue::flatbuf::StreamingQueueError err_code = queue::flatbuf::StreamingQueueError::OK;
    GetLastMsgIdRspMessage msg(get_msg->PeerActorId(), get_msg->ActorId(),
                               get_msg->QueueId(), down_queue->second->GetLastRecvSeqId(),
                               down_queue->second->GetLastRecvMsgId(), err_code);
    std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
  
    return buffer;
  }
}

bool QueueManager::PullPeerAsync(const ObjectID &queue_id, const ActorID &actor_id,
                   const ActorID &peer_actor_id, uint64_t start_seq_id) {
  PullRequestMessage msg(actor_id, peer_actor_id, queue_id, start_seq_id, /*async*/true);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer = transport_it->SendForResultWithRetry(std::move(buffer), 10, PULL_UPPSTREAM_DATA_TIMEOUT_MS);
  if (result_buffer == nullptr) {
    return false;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(result_msg->Type() ==
                  queue::flatbuf::MessageType::StreamingQueuePullResponseMsg);
  std::shared_ptr<PullResponseMessage> response_msg =
      std::dynamic_pointer_cast<PullResponseMessage>(result_msg);

  STREAMING_LOG(INFO) << "PullPeerAsync error: " << queue::flatbuf::EnumNameStreamingQueueError(response_msg->Error())
                      << " start_seq_id: " << start_seq_id;
  auto queue = downstream_queues_.find(queue_id);
  STREAMING_CHECK(queue!=downstream_queues_.end());

  if (response_msg->Error() == queue::flatbuf::StreamingQueueError::OK) {
    STREAMING_LOG(INFO) << "Set queue " << queue_id << " expect_seq_id to " << start_seq_id;
    queue->second->SetExpectSeqId(start_seq_id);
  }

  return response_msg->Error() == queue::flatbuf::StreamingQueueError::OK;
}

void QueueManager::ReleaseAllUpQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllUpQueues";
  upstream_state_ = StreamingQueueState::Destroyed;
  for (auto &it : upstream_queues_) {
    actors_.erase(it.first);
    out_transports_.erase(it.first);
  }
  upstream_queues_.clear();
}

void QueueManager::ReleaseAllDownQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues size: " << downstream_queues_.size();
  downstream_state_ = StreamingQueueState::Destroyed;
  for (auto &it : downstream_queues_) {
    actors_.erase(it.first);
    out_transports_.erase(it.first);
  }
  downstream_queues_.clear();
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues done: " << downstream_queues_.size();
}

}  // namespace streaming
}  // namespace ray