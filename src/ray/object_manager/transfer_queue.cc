#include "transfer_queue.h"

namespace ray {

bool TransferQueue::Empty() {
  ReadLock send_guard(send_mutex);
  ReadLock receive_guard(receive_mutex);
  return send_queue_.empty() && receive_queue_.empty();
}

uint64_t TransferQueue::SendCount() {
  ReadLock guard(send_mutex);
  return send_queue_.size();
}

uint64_t TransferQueue::ReceiveCount() {
  ReadLock guard(receive_mutex);
  return receive_queue_.size();
}

TransferQueue::TransferType TransferQueue::LastTransferType() {
  ReadLock guard(receive_mutex);
  return last_transfer_type_;
}

void TransferQueue::QueueSend(ClientID client_id, ObjectID object_id) {
  WriteLock guard(send_mutex);
  SendRequest req = {client_id, object_id};
  // TODO(hme): Use a set to speed this up.
  if (std::find(send_queue_.begin(), send_queue_.end(), req) != send_queue_.end()) {
    // already queued.
    return;
  }
  send_queue_.push_back(req);
}

TransferQueue::SendRequest TransferQueue::DequeueSend() {
  WriteLock guard(send_mutex);
  SendRequest req = send_queue_.front();
  send_queue_.pop_front();
  last_transfer_type_ = SEND;
  return req;
}

void TransferQueue::QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                                 uint64_t object_size,
                                 std::shared_ptr<ReceiverConnection> conn) {
  WriteLock guard(receive_mutex);
  ReceiveRequest req = {client_id, object_id, object_size, conn};
  if (std::find(receive_queue_.begin(), receive_queue_.end(), req) !=
      receive_queue_.end()) {
    // already queued.
    return;
  }
  receive_queue_.push_back(req);
}

TransferQueue::ReceiveRequest TransferQueue::DequeueReceive() {
  WriteLock guard(receive_mutex);
  ReceiveRequest req = receive_queue_.front();
  receive_queue_.pop_front();
  last_transfer_type_ = RECEIVE;
  return req;
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
