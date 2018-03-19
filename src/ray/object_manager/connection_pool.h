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

namespace asio = boost::asio;

namespace ray {

class ConnectionPool {
 private:
  // TODO(hme): make this a shared_ptr.
  ObjectDirectoryInterface *object_directory_;
  asio::io_service *connection_service_;
  ClientID client_id_;

  /// Note that (currently) receives take place on the main thread,
  /// and sends take place on a dedicated thread.
  using SenderMapType =
      std::unordered_map<ray::ClientID, std::vector<SenderConnection::pointer>,
                         ray::UniqueIDHasher>;
  SenderMapType message_send_connections_;
  SenderMapType transfer_send_connections_;
  SenderMapType available_message_send_connections_;
  SenderMapType available_transfer_send_connections_;

  using ReceiverMapType =
      std::unordered_map<ray::ClientID,
                         std::vector<std::shared_ptr<ObjectManagerClientConnection>>,
                         ray::UniqueIDHasher>;
  ReceiverMapType message_receive_connections_;
  ReceiverMapType transfer_receive_connections_;

 public:
  enum ConnectionType { MESSAGE = 0, TRANSFER };

  using SuccessCallback = std::function<void(SenderConnection::pointer)>;
  using FailureCallback = std::function<void()>;

  ConnectionPool(ObjectDirectoryInterface *object_directory,
                 asio::io_service *connection_service, const ClientID &client_id) {
    object_directory_ = object_directory;
    connection_service_ = connection_service;
    client_id_ = client_id;
  }

  void RegisterReceiver(ConnectionType type, const ClientID &client_id,
                        std::shared_ptr<ObjectManagerClientConnection> &conn) {
    switch (type) {
    case MESSAGE: {
      Add(message_receive_connections_, client_id, conn);
    } break;
    case TRANSFER: {
      Add(transfer_receive_connections_, client_id, conn);
    } break;
    }
  }

  void RemoveReceiver(ConnectionType type, const ClientID &client_id,
                      std::shared_ptr<ObjectManagerClientConnection> &conn) {
    switch (type) {
    case MESSAGE: {
      Remove(message_receive_connections_, client_id, conn);
    } break;
    case TRANSFER: {
      Remove(transfer_receive_connections_, client_id, conn);
    } break;
    }
    // TODO(hme): appropriately dispose of client connection.
  }

  ray::Status GetSender(ConnectionType type, ClientID client_id,
                        SuccessCallback success_callback,
                        FailureCallback failure_callback) {
    SenderMapType &avail_conn_map = (type == MESSAGE)
                                        ? available_message_send_connections_
                                        : available_transfer_send_connections_;
    if (Count(avail_conn_map, client_id) > 0) {
      success_callback(Borrow(avail_conn_map, client_id));
      return Status::OK();
    } else {
      return CreateConnection1(
          type, client_id,
          [this, type, client_id, success_callback](SenderConnection::pointer conn) {
            // add it to the connection map to maintain ownership.
            SenderMapType &conn_map = (type == MESSAGE) ? message_send_connections_
                                                        : transfer_send_connections_;
            Add(conn_map, client_id, conn);
            // add it to the available connections
            SenderMapType &avail_conn_map = (type == MESSAGE)
                                                ? available_message_send_connections_
                                                : available_transfer_send_connections_;
            Add(avail_conn_map, client_id, conn);
            // "borrow" the connection.
            success_callback(Borrow(avail_conn_map, client_id));
          },
          failure_callback);
    }
  }

  ray::Status ReleaseSender(ConnectionType type, SenderConnection::pointer conn) {
    SenderMapType &conn_map = (type == MESSAGE) ? available_message_send_connections_
                                                : available_transfer_send_connections_;
    Return(conn_map, conn->GetClientID(), conn);
    return ray::Status::OK();
  }

 private:
  void Add(ReceiverMapType &conn_map, const ClientID &client_id,
           std::shared_ptr<ObjectManagerClientConnection> conn) {
    if (conn_map.count(client_id) == 0) {
      conn_map[client_id] = std::vector<std::shared_ptr<ObjectManagerClientConnection>>();
    }
    conn_map[client_id].push_back(conn);
  }

  void Add(SenderMapType &conn_map, const ClientID &client_id,
           SenderConnection::pointer conn) {
    if (conn_map.count(client_id) == 0) {
      conn_map[client_id] = std::vector<SenderConnection::pointer>();
    }
    conn_map[client_id].push_back(conn);
  }

  void Remove(ReceiverMapType &conn_map, const ClientID &client_id,
              std::shared_ptr<ObjectManagerClientConnection> conn) {
    if (conn_map.count(client_id) == 0) {
      return;
    }
    std::vector<std::shared_ptr<ObjectManagerClientConnection>> &connections =
        conn_map[client_id];
    int64_t pos =
        std::find(connections.begin(), connections.end(), conn) - connections.begin();
    if (pos >= (int64_t)connections.size()) {
      return;
    }
    connections.erase(connections.begin() + pos);
  }

  uint64_t Count(SenderMapType &conn_map, const ClientID &client_id) {
    if (conn_map.count(client_id) == 0) {
      return 0;
    };
    return conn_map[client_id].size();
  };

  SenderConnection::pointer Borrow(SenderMapType &conn_map, const ClientID &client_id) {
    // RAY_LOG(INFO) << "Borrow";
    SenderConnection::pointer conn = conn_map[client_id].back();
    conn_map[client_id].pop_back();
    return conn;
  }

  void Return(SenderMapType &conn_map, const ClientID &client_id,
              SenderConnection::pointer conn) {
    // RAY_LOG(INFO) << "Return";
    conn_map[client_id].push_back(conn);
  }

  /// Asynchronously obtain a connection to client_id.
  /// If a connection to client_id already exists, the callback is invoked immediately.
  ray::Status CreateConnection1(ConnectionType type, const ClientID &client_id,
                                SuccessCallback success_callback,
                                FailureCallback failure_callback) {
    //    RAY_LOG(INFO) << "CreateConnection1 " << client_id;
    ray::Status status = Status::OK();
    status = object_directory_->GetInformation(
        client_id,
        [this, type, success_callback, failure_callback](RemoteConnectionInfo info) {
          RAY_CHECK_OK(CreateConnection2(type, info, success_callback, failure_callback));
        },
        [this, failure_callback](const Status &status) { failure_callback(); });
    return status;
  };

  /// Asynchronously create a connection to client_id.
  ray::Status CreateConnection2(ConnectionType type, RemoteConnectionInfo info,
                                SuccessCallback success_callback,
                                FailureCallback failure_callback) {
    //    RAY_LOG(INFO) << "CreateConnection2";
    SenderConnection::pointer conn = SenderConnection::Create(
        *connection_service_, info.client_id, info.ip, info.port);
    // Prepare client connection info buffer
    flatbuffers::FlatBufferBuilder fbb;
    bool is_transfer = (type == TRANSFER);
    auto message = CreateConnectClientMessage(fbb, fbb.CreateString(client_id_.binary()),
                                              is_transfer);
    fbb.Finish(message);
    // Send synchronously.
    (void)conn->WriteMessage(OMMessageType_ConnectClient, fbb.GetSize(),
                             fbb.GetBufferPointer());
    // The connection is ready, invoke callback with connection info.
    success_callback(conn);
    return Status::OK();
  };
};

}  // namespace ray

#endif  // RAY_CONNECTION_POOL_H
