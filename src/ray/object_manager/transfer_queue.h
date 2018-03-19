#ifndef RAY_TRANSFER_QUEUE_H
#define RAY_TRANSFER_QUEUE_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/id.h"
#include "ray/status.h"

#include "format/object_manager_generated.h"
#include "object_directory.h"
#include "object_manager_client_connection.h"

namespace asio = boost::asio;

namespace ray {

class TransferQueue {
 public:
  enum TransferType { SEND = 1, RECEIVE };

  struct SendContext {
    ClientID client_id;
    ObjectID object_id;
    int64_t object_size;
    uint8_t *data;
  };

  struct SendRequest {
   public:
    ClientID client_id;
    ObjectID object_id;
    friend bool operator==(const SendRequest &o1, const SendRequest &o2) {
      return o1.client_id == o2.client_id && o1.object_id == o2.object_id;
    }
  };

  struct ReceiveRequest {
    ClientID client_id;
    ObjectID object_id;
    uint64_t object_size;
    std::shared_ptr<ObjectManagerClientConnection> conn;
    friend bool operator==(const ReceiveRequest &o1, const ReceiveRequest &o2) {
      return o1.client_id == o2.client_id && o1.object_id == o2.object_id;
    }
  };

 private:
  std::deque<SendRequest> send_queue_;
  std::deque<ReceiveRequest> receive_queue_;
  std::unordered_map<ray::UniqueID, SendContext, ray::UniqueIDHasher> send_context_set_;

  TransferType last_transfer_type_;

 public:
  bool Empty() { return send_queue_.empty() && receive_queue_.empty(); }

  uint64_t SendCount() { return send_queue_.size(); }

  uint64_t ReceiveCount() { return receive_queue_.size(); }

  TransferType LastTransferType() { return last_transfer_type_; }

  void QueueSend(ClientID client_id, ObjectID object_id) {
    SendRequest req = {client_id, object_id};
    // TODO(hme): Use a set to speed this up.
    if (std::find(send_queue_.begin(), send_queue_.end(), req) != send_queue_.end()) {
      // already queued.
      return;
    }
    send_queue_.push_back(req);
  }

  SendRequest DequeueSend() {
    SendRequest req = send_queue_.front();
    send_queue_.pop_front();
    last_transfer_type_ = SEND;
    return req;
  }

  void QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                    uint64_t object_size,
                    std::shared_ptr<ObjectManagerClientConnection> conn) {
    ReceiveRequest req = {client_id, object_id, object_size, conn};
    if (std::find(receive_queue_.begin(), receive_queue_.end(), req) !=
        receive_queue_.end()) {
      // already queued.
      return;
    }
    receive_queue_.push_back(req);
  }

  ReceiveRequest DequeueReceive() {
    ReceiveRequest req = receive_queue_.front();
    receive_queue_.pop_front();
    last_transfer_type_ = RECEIVE;
    return req;
  }

  UniqueID AddContext(SendContext &context) {
    UniqueID id = UniqueID::from_random();
    send_context_set_.emplace(id, context);
    return id;
  }

  SendContext &GetContext(const UniqueID &id) { return send_context_set_[id]; }

  ray::Status RemoveContext(const UniqueID &id) {
    send_context_set_.erase(id);
    return Status::OK();
  }
};
}

#endif  // RAY_TRANSFER_QUEUE_H
