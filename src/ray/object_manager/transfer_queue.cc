#include "transfer_queue.h"

namespace ray {

bool TransferQueue::Empty() { return send_queue_.empty() && receive_queue_.empty(); }

uint64_t TransferQueue::SendCount() { return send_queue_.size(); }

uint64_t TransferQueue::ReceiveCount() { return receive_queue_.size(); }

TransferQueue::TransferType TransferQueue::LastTransferType() {
  return last_transfer_type_;
}

void TransferQueue::QueueSend(ClientID client_id, ObjectID object_id) {
  SendRequest req = {client_id, object_id};
  // TODO(hme): Use a set to speed this up.
  if (std::find(send_queue_.begin(), send_queue_.end(), req) != send_queue_.end()) {
    // already queued.
    return;
  }
  send_queue_.push_back(req);
}

TransferQueue::SendRequest TransferQueue::DequeueSend() {
  SendRequest req = send_queue_.front();
  send_queue_.pop_front();
  last_transfer_type_ = SEND;
  return req;
}

void TransferQueue::QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                                 uint64_t object_size,
                                 std::shared_ptr<ReceiverConnection> conn) {
  ReceiveRequest req = {client_id, object_id, object_size, conn};
  if (std::find(receive_queue_.begin(), receive_queue_.end(), req) !=
      receive_queue_.end()) {
    // already queued.
    return;
  }
  receive_queue_.push_back(req);
}

TransferQueue::ReceiveRequest TransferQueue::DequeueReceive() {
  ReceiveRequest req = receive_queue_.front();
  receive_queue_.pop_front();
  last_transfer_type_ = RECEIVE;
  return req;
}

UniqueID TransferQueue::AddContext(SendContext &context) {
  UniqueID id = UniqueID::from_random();
  send_context_set_.emplace(id, context);
  return id;
}

TransferQueue::SendContext &TransferQueue::GetContext(const UniqueID &id) {
  return send_context_set_[id];
}

ray::Status TransferQueue::RemoveContext(const UniqueID &id) {
  send_context_set_.erase(id);
  return Status::OK();
}
}  // namespace ray
