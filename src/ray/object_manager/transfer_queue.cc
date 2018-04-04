#include "ray/object_manager/transfer_queue.h"

namespace ray {

void TransferQueue::QueueSend(const ClientID &client_id, const ObjectID &object_id,
                              uint64_t data_size, uint64_t metadata_size,
                              uint64_t chunk_index, const RemoteConnectionInfo &info) {
  std::unique_lock<std::mutex> guard(send_mutex);
  SendRequest req = {client_id, object_id, data_size, metadata_size, chunk_index, info};
  // TODO(hme): Use a set to speed this up.
  if (std::find(send_queue_.begin(), send_queue_.end(), req) != send_queue_.end()) {
    // already queued.
    return;
  }
  send_queue_.push_back(req);
}

void TransferQueue::QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                                 uint64_t data_size, uint64_t metadata_size,
                                 uint64_t chunk_index,
                                 std::shared_ptr<TcpClientConnection> conn) {
  std::unique_lock<std::mutex> guard(receive_mutex);
  ReceiveRequest req = {client_id,     object_id,   data_size,
                        metadata_size, chunk_index, conn};
  if (std::find(receive_queue_.begin(), receive_queue_.end(), req) !=
      receive_queue_.end()) {
    // already queued.
    return;
  }
  receive_queue_.push_back(req);
}

bool TransferQueue::DequeueSendIfPresent(TransferQueue::SendRequest *send_ptr) {
  std::unique_lock<std::mutex> guard(send_mutex);
  if (send_queue_.empty()) {
    return false;
  }
  *send_ptr = send_queue_.front();
  send_queue_.pop_front();
  return true;
}

bool TransferQueue::DequeueReceiveIfPresent(TransferQueue::ReceiveRequest *receive_ptr) {
  std::unique_lock<std::mutex> guard(receive_mutex);
  if (receive_queue_.empty()) {
    return false;
  }
  *receive_ptr = receive_queue_.front();
  receive_queue_.pop_front();
  return true;
}

}  // namespace ray
