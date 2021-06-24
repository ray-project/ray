#include "queue/queue_handler.h"

#include "queue/utils.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

constexpr uint64_t COMMON_SYNC_CALL_TIMEOUTT_MS = 5 * 1000;

std::shared_ptr<UpstreamQueueMessageHandler>
    UpstreamQueueMessageHandler::upstream_handler_ = nullptr;
std::shared_ptr<DownstreamQueueMessageHandler>
    DownstreamQueueMessageHandler::downstream_handler_ = nullptr;

std::shared_ptr<Message> QueueMessageHandler::ParseMessage(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  uint8_t *bytes = buffer->Data();
  uint8_t *p_cur = bytes;
  uint32_t *magic_num = (uint32_t *)p_cur;
  STREAMING_CHECK(*magic_num == Message::MagicNum)
      << *magic_num << " " << Message::MagicNum;

  p_cur += sizeof(Message::MagicNum);
  queue::protobuf::StreamingQueueMessageType *type =
      (queue::protobuf::StreamingQueueMessageType *)p_cur;

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
  case queue::protobuf::StreamingQueuePullRequestMsgType:
    message = PullRequestMessage::FromBytes(bytes);
    break;
  case queue::protobuf::StreamingQueuePullResponseMsgType:
    message = PullResponseMessage::FromBytes(bytes);
    break;
  case queue::protobuf::StreamingQueueResendDataMsgType:
    message = ResendDataMessage::FromBytes(bytes);
    break;
  default:
    STREAMING_CHECK(false) << "nonsupport message type: "
                           << queue::protobuf::StreamingQueueMessageType_Name(*type);
    break;
  }

  return message;
}

void QueueMessageHandler::DispatchMessageAsync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  queue_service_.post(
      boost::bind(&QueueMessageHandler::DispatchMessageInternal, this, buffer, nullptr));
}

std::shared_ptr<LocalMemoryBuffer> QueueMessageHandler::DispatchMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  std::shared_ptr<LocalMemoryBuffer> result = nullptr;
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  queue_service_.post(
      boost::bind(&QueueMessageHandler::DispatchMessageInternal, this, buffer,
                  [&promise, &result](std::shared_ptr<LocalMemoryBuffer> rst) {
                    result = rst;
                    promise->Notify(ray::Status::OK());
                  }));
  Status st = promise->Wait();
  STREAMING_CHECK(st.ok());

  return result;
}

std::shared_ptr<Transport> QueueMessageHandler::GetOutTransport(
    const ObjectID &queue_id) {
  auto it = out_transports_.find(queue_id);
  if (it == out_transports_.end()) return nullptr;

  return it->second;
}

void QueueMessageHandler::SetPeerActorID(const ObjectID &queue_id,
                                         const ActorID &actor_id, RayFunction &async_func,
                                         RayFunction &sync_func) {
  actors_.emplace(queue_id, actor_id);
  out_transports_.emplace(queue_id, std::make_shared<ray::streaming::Transport>(
                                        actor_id, async_func, sync_func));
}

ActorID QueueMessageHandler::GetPeerActorID(const ObjectID &queue_id) {
  auto it = actors_.find(queue_id);
  STREAMING_CHECK(it != actors_.end());
  return it->second;
}

void QueueMessageHandler::Release() {
  actors_.clear();
  out_transports_.clear();
}

void QueueMessageHandler::Start() {
  queue_thread_ = std::thread(&QueueMessageHandler::QueueThreadCallback, this);
}

void QueueMessageHandler::Stop() {
  STREAMING_LOG(INFO) << "QueueMessageHandler Stop.";
  queue_service_.stop();
  if (queue_thread_.joinable()) {
    queue_thread_.join();
  }
}

void UpstreamQueueMessageHandler::Start() {
  STREAMING_LOG(INFO) << "UpstreamQueueMessageHandler::Start";
  QueueMessageHandler::Start();
  handle_service_thread_ = std::thread([this] { handler_service_.run(); });
}

void UpstreamQueueMessageHandler::Stop() {
  STREAMING_LOG(INFO) << "UpstreamQueueMessageHandler::Stop";
  handler_service_.stop();
  if (handle_service_thread_.joinable()) {
    handle_service_thread_.join();
  }
  QueueMessageHandler::Stop();
}

std::shared_ptr<UpstreamQueueMessageHandler> UpstreamQueueMessageHandler::CreateService(
    const ActorID &actor_id) {
  if (nullptr == upstream_handler_) {
    upstream_handler_ = std::make_shared<UpstreamQueueMessageHandler>(actor_id);
  }
  return upstream_handler_;
}

std::shared_ptr<UpstreamQueueMessageHandler> UpstreamQueueMessageHandler::GetService() {
  return upstream_handler_;
}

std::shared_ptr<WriterQueue> UpstreamQueueMessageHandler::CreateUpstreamQueue(
    const ObjectID &queue_id, const ActorID &peer_actor_id, uint64_t size) {
  STREAMING_LOG(INFO) << "CreateUpstreamQueue: " << queue_id << " " << actor_id_ << "->"
                      << peer_actor_id;
  std::shared_ptr<WriterQueue> queue = GetUpQueue(queue_id);
  if (queue != nullptr) {
    STREAMING_LOG(WARNING) << "Duplicate to create up queue." << queue_id;
    return queue;
  }

  queue = std::make_unique<streaming::WriterQueue>(queue_id, actor_id_, peer_actor_id,
                                                   size, GetOutTransport(queue_id));
  upstream_queues_[queue_id] = queue;

  return queue;
}

bool UpstreamQueueMessageHandler::UpstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetUpQueue(queue_id);
}

std::shared_ptr<streaming::WriterQueue> UpstreamQueueMessageHandler::GetUpQueue(
    const ObjectID &queue_id) {
  auto it = upstream_queues_.find(queue_id);
  if (it == upstream_queues_.end()) return nullptr;

  return it->second;
}

bool UpstreamQueueMessageHandler::CheckQueueSync(const ObjectID &queue_id) {
  ActorID peer_actor_id = GetPeerActorID(queue_id);
  STREAMING_LOG(INFO) << "CheckQueueSync queue_id: " << queue_id
                      << " peer_actor_id: " << peer_actor_id;

  CheckMessage msg(actor_id_, peer_actor_id, queue_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer = transport_it->SendForResultWithRetry(
      std::move(buffer), 10, COMMON_SYNC_CALL_TIMEOUTT_MS);
  if (result_buffer == nullptr) {
    return false;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(
      result_msg->Type() ==
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType);
  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::dynamic_pointer_cast<CheckRspMessage>(result_msg);
  STREAMING_LOG(INFO) << "CheckQueueSync return queue_id: " << check_rsp_msg->QueueId();
  STREAMING_CHECK(check_rsp_msg->PeerActorId() == actor_id_);

  return queue::protobuf::StreamingQueueError::OK == check_rsp_msg->Error();
}

void UpstreamQueueMessageHandler::WaitQueues(const std::vector<ObjectID> &queue_ids,
                                             int64_t timeout_ms,
                                             std::vector<ObjectID> &failed_queues) {
  failed_queues.insert(failed_queues.begin(), queue_ids.begin(), queue_ids.end());
  uint64_t start_time_us = current_time_ms();
  uint64_t current_time_us = start_time_us;
  while (!failed_queues.empty() && current_time_us < start_time_us + timeout_ms * 1000) {
    for (auto it = failed_queues.begin(); it != failed_queues.end();) {
      if (CheckQueueSync(*it)) {
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

void UpstreamQueueMessageHandler::DispatchMessageInternal(
    std::shared_ptr<LocalMemoryBuffer> buffer,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::shared_ptr<Message> msg = ParseMessage(buffer);
  STREAMING_LOG(DEBUG) << "UpstreamQueueMessageHandler::DispatchMessageInternal: "
                       << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
                       << " peer actorid: " << msg->PeerActorId() << " type: "
                       << queue::protobuf::StreamingQueueMessageType_Name(msg->Type());

  if (msg->Type() ==
      queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType) {
    OnNotify(std::dynamic_pointer_cast<NotificationMessage>(msg));
  } else if (msg->Type() ==
             queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType) {
    STREAMING_CHECK(false) << "Should not receive StreamingQueueCheckRspMsg";
  } else if (msg->Type() == queue::protobuf::StreamingQueueMessageType::
                                StreamingQueuePullRequestMsgType) {
    STREAMING_CHECK(callback) << "StreamingQueuePullRequestMsg "
                              << " qid: " << msg->QueueId() << " actorid "
                              << msg->ActorId()
                              << " peer actorid: " << msg->PeerActorId();
    OnPullRequest(std::dynamic_pointer_cast<PullRequestMessage>(msg), callback);
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::protobuf::StreamingQueueMessageType_Name(
                                  msg->Type());
  }
}

void UpstreamQueueMessageHandler::OnNotify(
    std::shared_ptr<NotificationMessage> notify_msg) {
  auto queue = GetUpQueue(notify_msg->QueueId());
  if (queue == nullptr) {
    STREAMING_LOG(WARNING) << "Can not find queue for "
                           << queue::protobuf::StreamingQueueMessageType_Name(
                                  notify_msg->Type())
                           << ", maybe queue has been destroyed, ignore it."
                           << " msg id: " << notify_msg->MsgId();
    return;
  }
  queue->OnNotify(notify_msg);
}

void UpstreamQueueMessageHandler::OnPullRequest(
    std::shared_ptr<PullRequestMessage> pull_msg,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  STREAMING_LOG(INFO) << "OnPullRequest";
  auto queue = upstream_queues_.find(pull_msg->QueueId());
  if (queue == upstream_queues_.end()) {
    STREAMING_LOG(INFO) << "Can not find queue " << pull_msg->QueueId();
    PullResponseMessage msg(pull_msg->PeerActorId(), pull_msg->ActorId(),
                            pull_msg->QueueId(), QUEUE_INVALID_SEQ_ID,
                            QUEUE_INVALID_SEQ_ID,
                            queue::protobuf::StreamingQueueError::QUEUE_NOT_EXIST, false);
    std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();
    callback(std::move(buffer));
    return;
  }

  queue->second->OnPull(pull_msg, handler_service_, callback);
}

void UpstreamQueueMessageHandler::ReleaseAllUpQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllUpQueues";
  upstream_queues_.clear();
  Release();
}

std::shared_ptr<DownstreamQueueMessageHandler>
DownstreamQueueMessageHandler::CreateService(const ActorID &actor_id) {
  if (nullptr == downstream_handler_) {
    STREAMING_LOG(INFO) << "DownstreamQueueMessageHandler::CreateService "
                        << " actorid: " << actor_id;
    downstream_handler_ = std::make_shared<DownstreamQueueMessageHandler>(actor_id);
  }
  return downstream_handler_;
}

std::shared_ptr<DownstreamQueueMessageHandler>
DownstreamQueueMessageHandler::GetService() {
  return downstream_handler_;
}

bool DownstreamQueueMessageHandler::DownstreamQueueExists(const ObjectID &queue_id) {
  return nullptr != GetDownQueue(queue_id);
}

std::shared_ptr<ReaderQueue> DownstreamQueueMessageHandler::CreateDownstreamQueue(
    const ObjectID &queue_id, const ActorID &peer_actor_id) {
  STREAMING_LOG(INFO) << "CreateDownstreamQueue: " << queue_id << " " << peer_actor_id
                      << "->" << actor_id_;
  auto it = downstream_queues_.find(queue_id);
  if (it != downstream_queues_.end()) {
    STREAMING_LOG(WARNING) << "Duplicate to create down queue!!!! " << queue_id;
    return it->second;
  }

  std::shared_ptr<streaming::ReaderQueue> queue =
      std::make_unique<streaming::ReaderQueue>(queue_id, actor_id_, peer_actor_id,
                                               GetOutTransport(queue_id));
  downstream_queues_[queue_id] = queue;
  return queue;
}

StreamingQueueStatus DownstreamQueueMessageHandler::PullQueue(
    const ObjectID &queue_id, uint64_t start_msg_id, bool &is_upstream_first_pull,
    uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "PullQueue queue_id: " << queue_id
                      << " start_msg_id: " << start_msg_id
                      << " is_upstream_first_pull: " << is_upstream_first_pull;
  uint64_t start_time = current_time_ms();
  uint64_t current_time = start_time;
  StreamingQueueStatus st = StreamingQueueStatus::OK;
  while (current_time < start_time + timeout_ms &&
         (st = PullPeerAsync(queue_id, start_msg_id, is_upstream_first_pull,
                             timeout_ms)) == StreamingQueueStatus::Timeout) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    current_time = current_time_ms();
  }
  return st;
}

std::shared_ptr<streaming::ReaderQueue> DownstreamQueueMessageHandler::GetDownQueue(
    const ObjectID &queue_id) {
  auto it = downstream_queues_.find(queue_id);
  if (it == downstream_queues_.end()) return nullptr;

  return it->second;
}

std::shared_ptr<LocalMemoryBuffer> DownstreamQueueMessageHandler::OnCheckQueue(
    std::shared_ptr<CheckMessage> check_msg) {
  queue::protobuf::StreamingQueueError err_code =
      queue::protobuf::StreamingQueueError::OK;

  auto down_queue = downstream_queues_.find(check_msg->QueueId());
  if (down_queue == downstream_queues_.end()) {
    STREAMING_LOG(WARNING) << "OnCheckQueue " << check_msg->QueueId() << " not found.";
    err_code = queue::protobuf::StreamingQueueError::QUEUE_NOT_EXIST;
  }

  CheckRspMessage msg(check_msg->PeerActorId(), check_msg->ActorId(),
                      check_msg->QueueId(), err_code);
  std::shared_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  return buffer;
}

void DownstreamQueueMessageHandler::ReleaseAllDownQueues() {
  STREAMING_LOG(INFO) << "ReleaseAllDownQueues size: " << downstream_queues_.size();
  downstream_queues_.clear();
  Release();
}

void DownstreamQueueMessageHandler::DispatchMessageInternal(
    std::shared_ptr<LocalMemoryBuffer> buffer,
    std::function<void(std::shared_ptr<LocalMemoryBuffer>)> callback) {
  std::shared_ptr<Message> msg = ParseMessage(buffer);
  STREAMING_LOG(DEBUG) << "DownstreamQueueMessageHandler::DispatchMessageInternal: "
                       << " qid: " << msg->QueueId() << " actorid " << msg->ActorId()
                       << " peer actorid: " << msg->PeerActorId() << " type: "
                       << queue::protobuf::StreamingQueueMessageType_Name(msg->Type());

  if (msg->Type() ==
      queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType) {
    OnData(std::dynamic_pointer_cast<DataMessage>(msg));
  } else if (msg->Type() ==
             queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType) {
    std::shared_ptr<LocalMemoryBuffer> check_result =
        this->OnCheckQueue(std::dynamic_pointer_cast<CheckMessage>(msg));
    if (callback != nullptr) {
      callback(check_result);
    }
  } else if (msg->Type() == queue::protobuf::StreamingQueueMessageType::
                                StreamingQueueResendDataMsgType) {
    auto queue = downstream_queues_.find(msg->QueueId());
    if (queue == downstream_queues_.end()) {
      std::shared_ptr<ResendDataMessage> data_msg =
          std::dynamic_pointer_cast<ResendDataMessage>(msg);
      STREAMING_LOG(DEBUG) << "Can not find queue for "
                           << queue::protobuf::StreamingQueueMessageType_Name(msg->Type())
                           << ", maybe queue has been destroyed, ignore it."
                           << " seq id: " << data_msg->SeqId();
      return;
    }
    std::shared_ptr<ResendDataMessage> resend_data_msg =
        std::dynamic_pointer_cast<ResendDataMessage>(msg);

    queue->second->OnResendData(resend_data_msg);
  } else {
    STREAMING_CHECK(false) << "message type should be added: "
                           << queue::protobuf::StreamingQueueMessageType_Name(
                                  msg->Type());
  }
}

void DownstreamQueueMessageHandler::OnData(std::shared_ptr<DataMessage> msg) {
  auto queue = GetDownQueue(msg->QueueId());
  if (queue == nullptr) {
    STREAMING_LOG(WARNING) << "Can not find queue for "
                           << queue::protobuf::StreamingQueueMessageType_Name(msg->Type())
                           << ", maybe queue has been destroyed, ignore it."
                           << " seq id: " << msg->SeqId();
    return;
  }

  QueueItem item(msg);
  queue->OnData(item);
}

StreamingQueueStatus DownstreamQueueMessageHandler::PullPeerAsync(
    const ObjectID &queue_id, uint64_t start_msg_id, bool &is_upstream_first_pull,
    uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "PullPeerAsync queue_id: " << queue_id
                      << " start_msg_id: " << start_msg_id;
  auto queue = GetDownQueue(queue_id);
  STREAMING_CHECK(queue != nullptr);
  STREAMING_LOG(INFO) << "PullPeerAsync "
                      << " actorid: " << queue->GetActorID();
  PullRequestMessage msg(queue->GetActorID(), queue->GetPeerActorID(), queue_id,
                         start_msg_id);
  std::unique_ptr<LocalMemoryBuffer> buffer = msg.ToBytes();

  auto transport_it = GetOutTransport(queue_id);
  STREAMING_CHECK(transport_it != nullptr);
  std::shared_ptr<LocalMemoryBuffer> result_buffer =
      transport_it->SendForResultWithRetry(std::move(buffer), 1, timeout_ms);
  if (result_buffer == nullptr) {
    return StreamingQueueStatus::Timeout;
  }

  std::shared_ptr<Message> result_msg = ParseMessage(result_buffer);
  STREAMING_CHECK(
      result_msg->Type() ==
      queue::protobuf::StreamingQueueMessageType::StreamingQueuePullResponseMsgType);
  std::shared_ptr<PullResponseMessage> response_msg =
      std::dynamic_pointer_cast<PullResponseMessage>(result_msg);

  STREAMING_LOG(INFO) << "PullPeerAsync error: "
                      << queue::protobuf::StreamingQueueError_Name(response_msg->Error())
                      << " start_msg_id: " << start_msg_id;

  is_upstream_first_pull = response_msg->IsUpstreamFirstPull();
  if (response_msg->Error() == queue::protobuf::StreamingQueueError::OK) {
    STREAMING_LOG(INFO) << "Set queue " << queue_id << " expect_seq_id to "
                        << response_msg->SeqId();
    return StreamingQueueStatus::OK;
  } else if (response_msg->Error() == queue::protobuf::StreamingQueueError::DATA_LOST) {
    return StreamingQueueStatus::DataLost;
  } else if (response_msg->Error() ==
             queue::protobuf::StreamingQueueError::NO_VALID_DATA) {
    return StreamingQueueStatus::NoValidData;
  } else {  // QUEUE_NOT_EXIST
    return StreamingQueueStatus::Timeout;
  }
}

}  // namespace streaming
}  // namespace ray