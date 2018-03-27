#ifndef RAY_CONNECTION_POOL_H
#define RAY_CONNECTION_POOL_H

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
#include <mutex>

namespace asio = boost::asio;

namespace ray {

class ConnectionPool {
 public:
  /// Callbacks for GetSender.
  using SuccessCallback = std::function<void(SenderConnection::pointer)>;
  using FailureCallback = std::function<void()>;

  /// Connection type to distinguish between message and transfer connections.
  enum ConnectionType { MESSAGE = 0, TRANSFER };

  /// Connection pool for all connections needed by the ObjectManager.
  ///
  /// \param object_directory The object directory, used for obtaining
  /// remote object manager connection information.
  /// \param connection_service The io_service the connection pool should use
  /// to create new connections.
  /// \param client_id The ClientID of this node.
  ConnectionPool(ObjectDirectoryInterface *object_directory,
                 asio::io_service *connection_service, const ClientID &client_id);

  /// Register a receiver connection.
  ///
  /// \param type The type of connection.
  /// \param client_id The ClientID of the remote object manager.
  /// \param conn The actual connection.
  void RegisterReceiver(ConnectionType type, const ClientID &client_id,
                        std::shared_ptr<ReceiverConnection> &conn);

  /// Remove a receiver connection.
  ///
  /// \param type The type of connection.
  /// \param client_id The ClientID of the remote object manager.
  /// \param conn The actual connection.
  void RemoveReceiver(ConnectionType type, const ClientID &client_id,
                      std::shared_ptr<ReceiverConnection> &conn);

  /// Get a sender connection from the connection pool.
  /// The connection must be released or removed when the operation for which the
  /// connection was obtained is completed.
  ///
  /// \param type The type of connection.
  /// \param client_id The ClientID of the remote object manager.
  /// \param success_callback The callback invoked when a sender is available.
  /// \param failure_callback The callback invoked if this method fails.
  /// \return Status of invoking this method.
  ray::Status GetSender(ConnectionType type, ClientID client_id,
                        SuccessCallback success_callback,
                        FailureCallback failure_callback);

  /// Releasex a sender connection, allowing it to be used by another operation.
  ///
  /// \param type The type of connection.
  /// \param conn The actual connection.
  /// \return Status of invoking this method.
  ray::Status ReleaseSender(ConnectionType type, SenderConnection::pointer conn);

  /// Remove a sender connection. This is invoked if the connection is no longer
  /// usable.
  /// \param type The type of connection.
  /// \param conn The actual connection.
  /// \return Status of invoking this method.
  // TODO(hme): Implement with error handling.
  ray::Status RemoveSender(ConnectionType type, SenderConnection::pointer conn);

  /// This object cannot be copied for thread-safety.
  ConnectionPool &operator=(const ConnectionPool &o) {
    throw std::runtime_error("Can't copy ConnectionPool.");
  }

 private:
  /// A container type that maps ClientID to a connection type.
  using SenderMapType =
      std::unordered_map<ray::ClientID, std::vector<SenderConnection::pointer>,
                         ray::UniqueIDHasher>;
  using ReceiverMapType =
      std::unordered_map<ray::ClientID, std::vector<std::shared_ptr<ReceiverConnection>>,
                         ray::UniqueIDHasher>;

  /// Adds a receiver for ClientID to the given map.
  void Add(ReceiverMapType &conn_map, const ClientID &client_id,
           std::shared_ptr<ReceiverConnection> conn);

  /// Adds a sender for ClientID to the given map.
  void Add(SenderMapType &conn_map, const ClientID &client_id,
           SenderConnection::pointer conn);

  /// Removes the given receiver for ClientID from the given map.
  void Remove(ReceiverMapType &conn_map, const ClientID &client_id,
              std::shared_ptr<ReceiverConnection> conn);

  /// Returns the count of sender connections to ClientID.
  uint64_t Count(SenderMapType &conn_map, const ClientID &client_id);

  /// Removes a sender connection to ClientID from the pool of available connections.
  /// This method assumes conn_map has available connections to ClientID.
  SenderConnection::pointer Borrow(SenderMapType &conn_map, const ClientID &client_id);

  /// Returns a sender connection to ClientID to the pool of available connections.
  void Return(SenderMapType &conn_map, const ClientID &client_id,
              SenderConnection::pointer conn);

  /// Asynchronously obtain a connection to client_id.
  /// If a connection to client_id already exists, the callback is invoked immediately.
  ray::Status CreateConnection1(ConnectionType type, const ClientID &client_id,
                                SuccessCallback success_callback,
                                FailureCallback failure_callback);

  /// Asynchronously create a connection to client_id.
  ray::Status CreateConnection2(ConnectionType type, RemoteConnectionInfo info,
                                SuccessCallback success_callback,
                                FailureCallback failure_callback);

  // TODO(hme): Optimize with separate mutex per collection.
  std::mutex connection_mutex;
  // TODO(hme): make this a shared_ptr.
  ObjectDirectoryInterface *object_directory_;
  asio::io_service *connection_service_;
  ClientID client_id_;

  SenderMapType message_send_connections_;
  SenderMapType transfer_send_connections_;
  SenderMapType available_message_send_connections_;
  SenderMapType available_transfer_send_connections_;

  ReceiverMapType message_receive_connections_;
  ReceiverMapType transfer_receive_connections_;
};

}  // namespace ray

#endif  // RAY_CONNECTION_POOL_H
