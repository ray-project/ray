#include "ray/object_manager/transfer_queue.h"

namespace ray {

void TransferQueue::QueueSend(const ClientID &client_id, const ObjectID &object_id,
                              const RemoteConnectionInfo &info) {
  WriteLock guard(send_mutex);
  SendRequest req = {client_id, object_id, info};
  // TODO(hme): Use a set to speed this up.
  if (std::find(send_queue_.begin(), send_queue_.end(), req) != send_queue_.end()) {
    // already queued.
    return;
  }
  send_queue_.push_back(req);
}

void TransferQueue::QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                                 uint64_t object_size,
                                 std::shared_ptr<TcpClientConnection> conn) {
  WriteLock guard(receive_mutex);
  ReceiveRequest req = {client_id, object_id, object_size, conn};
  if (std::find(receive_queue_.begin(), receive_queue_.end(), req) !=
      receive_queue_.end()) {
    // already queued.
    return;
  }
  receive_queue_.push_back(req);
}

bool TransferQueue::DequeueSendIfPresent(TransferQueue::SendRequest *send_ptr) {
  WriteLock guard(send_mutex);
  if (send_queue_.empty()) {
    return false;
  }
  *send_ptr = send_queue_.front();
  send_queue_.pop_front();
  return true;
}

bool TransferQueue::DequeueReceiveIfPresent(TransferQueue::ReceiveRequest *receive_ptr) {
  WriteLock guard(receive_mutex);
  if (receive_queue_.empty()) {
    return false;
  }
  *receive_ptr = receive_queue_.front();
  receive_queue_.pop_front();
  return true;
}

UniqueID TransferQueue::AddContext(SendContext &context) {
  WriteLock guard(context_mutex);
  UniqueID id = UniqueID::from_random();
  send_context_set_.emplace(id, context);
  return id;
}

TransferQueue::SendContext &TransferQueue::GetContext(const UniqueID &id) {
  ReadLock guard(context_mutex);
  return send_context_set_[id];
}

ray::Status TransferQueue::RemoveContext(const UniqueID &id) {
  WriteLock guard(context_mutex);
  send_context_set_.erase(id);
  return Status::OK();
}
}  // namespace ray
