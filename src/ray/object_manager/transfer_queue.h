#ifndef RAY_OBJECT_MANAGER_TRANSFER_QUEUE_H
#define RAY_OBJECT_MANAGER_TRANSFER_QUEUE_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/id.h"
#include "ray/status.h"

#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/object_manager_client_connection.h"

namespace ray {

class TransferQueue {
 public:
  enum TransferType { SEND = 1, RECEIVE };

  /// The structure used in the send queue.
  struct SendRequest {
    ClientID client_id;
    ObjectID object_id;
    uint64_t data_size;
    uint64_t metadata_size;
    uint64_t chunk_index;
    RemoteConnectionInfo connection_info;
    bool operator==(const SendRequest &rhs) const {
      return client_id == rhs.client_id && object_id == rhs.object_id &&
             chunk_index == rhs.chunk_index;
    }
  };

  /// The structure used in the receive queue.
  struct ReceiveRequest {
    ClientID client_id;
    ObjectID object_id;
    uint64_t data_size;
    uint64_t metadata_size;
    uint64_t chunk_index;
    std::shared_ptr<TcpClientConnection> conn;
    bool operator==(const ReceiveRequest &rhs) const {
      return client_id == rhs.client_id && object_id == rhs.object_id &&
             chunk_index == rhs.chunk_index;
      ;
    }
  };

  TransferQueue() = default;

  /// Queues a send.
  ///
  /// \param client_id The ClientID to which the object needs to be sent.
  /// \param object_id The ObjectID of the object to be sent.
  void QueueSend(const ClientID &client_id, const ObjectID &object_id, uint64_t data_size,
                 uint64_t metadata_size, uint64_t chunk_index,
                 const RemoteConnectionInfo &info);

  /// If send_queue_ is not empty, removes a SendRequest from send_queue_ and assigns
  /// it to send_ptr. The queue is FIFO.
  ///
  /// \param send_ptr A pointer to an empty SendRequest.
  /// \return A bool indicating whether the queue was empty at the time this method
  /// was invoked.
  bool DequeueSendIfPresent(TransferQueue::SendRequest *send_ptr);

  /// Queues a receive.
  ///
  /// \param client_id The ClientID from which the object is being received.
  /// \param object_id The ObjectID of the object to be received.
  void QueueReceive(const ClientID &client_id, const ObjectID &object_id,
                    uint64_t data_size, uint64_t metadata_size, uint64_t chunk_index,
                    std::shared_ptr<TcpClientConnection> conn);

  /// If receive_queue_ is not empty, removes a ReceiveRequest from receive_queue_ and
  /// assigns it to receive_ptr. The queue is FIFO.
  ///
  /// \param receive_ptr A pointer to an empty ReceiveRequest.
  /// \return A bool indicating whether the queue was empty at the time this method
  /// was invoked.
  bool DequeueReceiveIfPresent(TransferQueue::ReceiveRequest *receive_ptr);

  /// This object cannot be copied for thread-safety.
  RAY_DISALLOW_COPY_AND_ASSIGN(TransferQueue);

 private:
  std::mutex send_mutex;
  std::mutex receive_mutex;
  std::deque<SendRequest> send_queue_;
  std::deque<ReceiveRequest> receive_queue_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_TRANSFER_QUEUE_H
