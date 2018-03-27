#ifndef RAY_TRANSFER_QUEUE_H
#define RAY_TRANSFER_QUEUE_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <thread>
#include <mutex>

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

  /// Context maintained during an object send.
  struct SendContext {
    ClientID client_id;
    ObjectID object_id;
    int64_t object_size;
    uint8_t *data;
  };

  /// The structure used in the send queue.
  struct SendRequest {
    ClientID client_id;
    ObjectID object_id;
    friend bool operator==(const SendRequest &o1, const SendRequest &o2) {
      return o1.client_id == o2.client_id && o1.object_id == o2.object_id;
    }
  };

  /// The structure used in the receive queue.
  struct ReceiveRequest {
    ClientID client_id;
    ObjectID object_id;
    uint64_t object_size;
    std::shared_ptr<ReceiverConnection> conn;
    friend bool operator==(const ReceiveRequest &o1, const ReceiveRequest &o2) {
      return o1.client_id == o2.client_id && o1.object_id == o2.object_id;
    }
  };

  /// \return Whether the transfer queue is empty.
  bool Empty();

  /// \return The number of sends in the transfer queue.
  uint64_t SendCount();

  /// \return The number of receives in the transfer queue.
  uint64_t ReceiveCount();

  /// \return Indicator of the last transfer type to be dequeued from the queue.
  TransferType LastTransferType();

  /// Queues a send.
  ///
  /// \param client_id The ClientID to which the object needs to be sent.
  /// \param object_id The ObjectID of the object to be sent.
  void QueueSend(ClientID client_id, ObjectID object_id);

  /// \return Removes a SendRequest from the send queue. This queue is FIFO.
  SendRequest DequeueSend();

  /// Queues a receive.
  ///
  /// \param client_id The ClientID from which the object is being received.
  /// \param object_id The ObjectID of the object to be received.
  void QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                    uint64_t object_size, std::shared_ptr<ReceiverConnection> conn);

  /// \return Removes a ReceiveRequest from the receive queue. This queue is FIFO.
  ReceiveRequest DequeueReceive();

  /// Maintain ownership over SendContext for sends in transit.
  ///
  /// \param context The context to maintain.
  /// \return A unique identifier identifying the context that was added.
  UniqueID AddContext(SendContext &context);

  /// Gets the SendContext associated with the given id.
  ///
  /// \param id The unique identifier of the context.
  /// \return The context.
  SendContext &GetContext(const UniqueID &id);

  /// Removes the context associated with the given id.
  ///
  /// \param id The unique identifier of the context.
  /// \return The status of invoking this method.
  ray::Status RemoveContext(const UniqueID &id);

  /// This object cannot be copied for thread-safety.
  TransferQueue &operator=(const TransferQueue &o) {
    throw std::runtime_error("Can't copy TransferQueue.");
  }

 private:
  // TODO(hme): make this a shared mutex.
  typedef std::mutex Lock;
  typedef std::unique_lock<Lock> WriteLock;
  // TODO(hme): make this a shared lock.
  typedef std::unique_lock<Lock> ReadLock;
  Lock send_mutex;
  Lock receive_mutex;
  Lock context_mutex;


  std::deque<SendRequest> send_queue_;
  std::deque<ReceiveRequest> receive_queue_;
  std::unordered_map<ray::UniqueID, SendContext, ray::UniqueIDHasher> send_context_set_;
  TransferType last_transfer_type_;
};
}  // namespace ray

#endif  // RAY_TRANSFER_QUEUE_H
