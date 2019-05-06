#ifndef RAY_OBJECT_MANAGER_CONNECTION_POOL_H
#define RAY_OBJECT_MANAGER_CONNECTION_POOL_H

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

#include <mutex>
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/object_manager_client_connection.h"
#include "ray/stats/stats.h"

namespace asio = boost::asio;

namespace ray {

class ConnectionPool {
 public:
  /// Callbacks for GetSender.
  using SuccessCallback = std::function<void(std::shared_ptr<SenderConnection>)>;
  using FailureCallback = std::function<void()>;

  /// Connection type to distinguish between message and transfer connections.
  enum class ConnectionType : int { MESSAGE = 0, TRANSFER };

  /// Connection pool for all connections needed by the ObjectManager.
  ConnectionPool();

  /// Register a receiver connection.
  ///
  /// \param type The type of connection.
  /// \param client_id The ClientID of the remote object manager.
  /// \param conn The actual connection.
  void RegisterReceiver(ConnectionType type, const ClientID &client_id,
                        std::shared_ptr<TcpClientConnection> &conn);

  /// Remove a receiver connection.
  ///
  /// \param conn The actual connection.
  void RemoveReceiver(std::shared_ptr<TcpClientConnection> conn);

  /// Register a receiver connection.
  ///
  /// \param type The type of connection.
  /// \param client_id The ClientID of the remote object manager.
  /// \param conn The actual connection.
  void RegisterSender(ConnectionType type, const ClientID &client_id,
                      std::shared_ptr<SenderConnection> &conn);

  /// Remove a sender connection.
  ///
  /// \param conn The actual connection.
  void RemoveSender(const std::shared_ptr<SenderConnection> &conn);

  /// Get a sender connection from the connection pool.
  /// The connection must be released or removed when the operation for which the
  /// connection was obtained is completed. If the connection pool is empty, the
  /// connection pointer passed in is set to a null pointer.
  ///
  /// \param[in] type The type of connection.
  /// \param[in] client_id The ClientID of the remote object manager.
  /// \param[out] conn An empty pointer to a shared pointer.
  /// \return Void.
  void GetSender(ConnectionType type, const ClientID &client_id,
                 std::shared_ptr<SenderConnection> *conn);

  /// Releases a sender connection, allowing it to be used by another operation.
  ///
  /// \param type The type of connection.
  /// \param conn The actual connection.
  /// \return Void.
  void ReleaseSender(ConnectionType type, std::shared_ptr<SenderConnection> &conn);

  // TODO(hme): Implement with error handling.
  /// Remove a sender connection. This is invoked if the connection is no longer
  /// usable.
  ///
  /// \param type The type of connection.
  /// \param conn The actual connection.
  /// \return Status of invoking this method.
  ray::Status RemoveSender(ConnectionType type, std::shared_ptr<SenderConnection> conn);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

  /// This object cannot be copied for thread-safety.
  RAY_DISALLOW_COPY_AND_ASSIGN(ConnectionPool);

 private:
  /// A container type that maps ClientID to a connection type.
  using SenderMapType =
      std::unordered_map<ray::ClientID, std::vector<std::shared_ptr<SenderConnection>>>;
  using ReceiverMapType =
      std::unordered_map<ray::ClientID,
                         std::vector<std::shared_ptr<TcpClientConnection>>>;

  /// Adds a receiver for ClientID to the given map.
  void Add(ReceiverMapType &conn_map, const ClientID &client_id,
           std::shared_ptr<TcpClientConnection> conn);

  /// Adds a sender for ClientID to the given map.
  void Add(SenderMapType &conn_map, const ClientID &client_id,
           std::shared_ptr<SenderConnection> conn);

  /// Removes the given receiver for ClientID from the given map.
  void Remove(ReceiverMapType &conn_map, const ClientID &client_id,
              std::shared_ptr<TcpClientConnection> &conn);

  /// Removes the given sender for ClientID from the given map.
  void Remove(SenderMapType &conn_map, const ClientID &client_id,
              const std::shared_ptr<SenderConnection> &conn);

  /// Returns the count of sender connections to ClientID.
  uint64_t Count(SenderMapType &conn_map, const ClientID &client_id);

  /// Removes a sender connection to ClientID from the pool of available connections.
  /// This method assumes conn_map has available connections to ClientID.
  std::shared_ptr<SenderConnection> Borrow(SenderMapType &conn_map,
                                           const ClientID &client_id);

  /// Returns a sender connection to ClientID to the pool of available connections.
  void Return(SenderMapType &conn_map, const ClientID &client_id,
              std::shared_ptr<SenderConnection> conn);

  // TODO(hme): Optimize with separate mutex per collection.
  std::mutex connection_mutex;

  SenderMapType message_send_connections_;
  SenderMapType transfer_send_connections_;
  SenderMapType available_message_send_connections_;
  SenderMapType available_transfer_send_connections_;

  ReceiverMapType message_receive_connections_;
  ReceiverMapType transfer_receive_connections_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_CONNECTION_POOL_H
