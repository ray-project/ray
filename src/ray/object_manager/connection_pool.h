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
#include "object_manager_client_connection.h"
#include "object_directory.h"

namespace asio = boost::asio;

namespace ray {

class ConnectionPool {
  
public:

  enum ConnectionType {
    MESSAGE=0,
    TRANSFER
  };

  enum ConnectionDirection {
    SEND=0,
    RECIEVE
  };

  using SuccessCallback = std::function<void(SenderConnection::pointer)>;
  using FailureCallback = std::function<void()>;
  
  ConnectionPool(ObjectDirectoryInterface *object_directory,
                 asio::io_service *connection_service,
                 const ClientID &client_id){
    object_directory_ = object_directory;
    connection_service_ = connection_service;
    client_id_ = client_id;
  }

  void RegisterReceiver(ConnectionType type, const ClientID &client_id, std::shared_ptr<ObjectManagerClientConnection> &conn){
    switch(type) {
      case MESSAGE: {
        message_receive_connections_[client_id] = conn;
      } break;
      case TRANSFER: {
        transfer_receive_connections_[client_id] = conn;
      } break;
    }
  }

  void RemoveReceiver(ConnectionType type, const ClientID &client_id){
    switch(type) {
      case MESSAGE: {
        message_receive_connections_.erase(client_id);
      } break;
      case TRANSFER: {
        transfer_receive_connections_.erase(client_id);
      } break;
    }
    // TODO(hme): appropriately dispose of client connection.
  }

  ray::Status GetSender(ConnectionType type,
                        ClientID client_id,
                        SuccessCallback success_callback,
                        FailureCallback failure_callback){
    return GetConnection(type, client_id, success_callback, failure_callback);
  }

 private:

  // TODO(hme): make this a shared_ptr.
  ObjectDirectoryInterface *object_directory_;
  asio::io_service *connection_service_;
  ClientID client_id_;

  /// Asynchronously obtain a connection to client_id.
  /// If a connection to client_id already exists, the callback is invoked immediately.
  ray::Status GetConnection(ConnectionType type,
                            const ClientID &client_id,
                            SuccessCallback success_callback,
                            FailureCallback failure_callback) {
    ray::Status status = Status::OK();
    SenderMapType &send_connections = (type == MESSAGE)? message_send_connections_ : transfer_send_connections_;
    if (send_connections.count(client_id) > 0) {
      success_callback(send_connections[client_id]);
    } else {
      status = object_directory_->GetInformation(
          client_id,
          [this, type, success_callback, failure_callback](RemoteConnectionInfo info) {
            RAY_CHECK_OK(CreateConnection(type, info, success_callback, failure_callback));
          },
          [this, failure_callback](const Status &status) {
            failure_callback();
          });
    }
    return status;
  };

  /// Asynchronously create a connection to client_id.
  ray::Status CreateConnection(ConnectionType type,
                               RemoteConnectionInfo info,
                               SuccessCallback success_callback,
                               FailureCallback failure_callback) {
    SenderMapType &send_connections = (type == MESSAGE)? message_send_connections_ : transfer_send_connections_;
    send_connections.emplace(info.client_id, SenderConnection::Create(*connection_service_, info.ip, info.port));
    // Prepare client connection info buffer
    flatbuffers::FlatBufferBuilder fbb;
    bool is_transfer = (type == TRANSFER);
    auto message = CreateConnectClientMessage(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
    fbb.Finish(message);
    // Send synchronously.
    SenderConnection::pointer conn = send_connections[info.client_id];
    conn->WriteMessage(OMMessageType_ConnectClient, fbb.GetSize(), fbb.GetBufferPointer());
    // The connection is ready, invoke callback with connection info.
    success_callback(send_connections[info.client_id]);
    return Status::OK();
  };

  /// Note that (currently) receives take place on the main thread,
  /// and sends take place on a dedicated thread.
  using SenderMapType = std::unordered_map<ray::ClientID, SenderConnection::pointer, ray::UniqueIDHasher>;
  SenderMapType message_send_connections_;
  SenderMapType transfer_send_connections_;

  using ReceiverMapType = std::unordered_map<ray::ClientID, std::shared_ptr<ObjectManagerClientConnection>, ray::UniqueIDHasher>;
  ReceiverMapType message_receive_connections_;
  ReceiverMapType transfer_receive_connections_;

};

} // namespace ray

#endif //RAY_CONNECTION_POOL_H
