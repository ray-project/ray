#include "connection_pool.h"

namespace ray {

ConnectionPool::ConnectionPool(ObjectDirectoryInterface *object_directory,
                               asio::io_service *connection_service,
                               const ClientID &client_id) {
  object_directory_ = object_directory;
  connection_service_ = connection_service;
  client_id_ = client_id;
}

void ConnectionPool::RegisterReceiver(ConnectionType type, const ClientID &client_id,
                                      std::shared_ptr<ReceiverConnection> &conn) {
  switch (type) {
  case ConnectionType::MESSAGE: {
    Add(message_receive_connections_, client_id, conn);
  } break;
  case ConnectionType::TRANSFER: {
    Add(transfer_receive_connections_, client_id, conn);
  } break;
  }
}

void ConnectionPool::RemoveReceiver(ConnectionType type, const ClientID &client_id,
                                    std::shared_ptr<ReceiverConnection> &conn) {
  switch (type) {
  case ConnectionType::MESSAGE: {
    Remove(message_receive_connections_, client_id, conn);
  } break;
  case ConnectionType::TRANSFER: {
    Remove(transfer_receive_connections_, client_id, conn);
  } break;
  }
  // TODO(hme): appropriately dispose of client connection.
}

ray::Status ConnectionPool::GetSender(ConnectionType type, const ClientID &client_id,
                                      SuccessCallback success_callback,
                                      FailureCallback failure_callback) {
  SenderMapType &avail_conn_map = (type == ConnectionType::MESSAGE)
                                      ? available_message_send_connections_
                                      : available_transfer_send_connections_;
  if (Count(avail_conn_map, client_id) > 0) {
    success_callback(Borrow(avail_conn_map, client_id));
    return Status::OK();
  } else {
    return GetNewConnection(
        type, client_id,
        [this, type, client_id, success_callback](SenderConnection::pointer conn) {
          // add it to the connection map to maintain ownership.
          SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                        ? message_send_connections_
                                        : transfer_send_connections_;
          Add(conn_map, client_id, conn);
          // add it to the available connections
          SenderMapType &avail_conn_map = (type == ConnectionType::MESSAGE)
                                              ? available_message_send_connections_
                                              : available_transfer_send_connections_;
          Add(avail_conn_map, client_id, conn);
          // "borrow" the connection.
          success_callback(Borrow(avail_conn_map, client_id));
        },
        failure_callback);
  }
}

ray::Status ConnectionPool::ReleaseSender(ConnectionType type,
                                          SenderConnection::pointer conn) {
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? available_message_send_connections_
                                : available_transfer_send_connections_;
  Return(conn_map, conn->GetClientID(), conn);
  return ray::Status::OK();
}

void ConnectionPool::Add(ReceiverMapType &conn_map, const ClientID &client_id,
                         std::shared_ptr<ReceiverConnection> conn) {
  if (conn_map.count(client_id) == 0) {
    conn_map[client_id] = std::vector<std::shared_ptr<ReceiverConnection>>();
  }
  conn_map[client_id].push_back(conn);
}

void ConnectionPool::Add(SenderMapType &conn_map, const ClientID &client_id,
                         SenderConnection::pointer conn) {
  if (conn_map.count(client_id) == 0) {
    conn_map[client_id] = std::vector<SenderConnection::pointer>();
  }
  conn_map[client_id].push_back(conn);
}

void ConnectionPool::Remove(ReceiverMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<ReceiverConnection> conn) {
  if (conn_map.count(client_id) == 0) {
    return;
  }
  std::vector<std::shared_ptr<ReceiverConnection>> &connections = conn_map[client_id];
  int64_t pos =
      std::find(connections.begin(), connections.end(), conn) - connections.begin();
  if (pos >= (int64_t)connections.size()) {
    return;
  }
  connections.erase(connections.begin() + pos);
}

uint64_t ConnectionPool::Count(SenderMapType &conn_map, const ClientID &client_id) {
  if (conn_map.count(client_id) == 0) {
    return 0;
  };
  return conn_map[client_id].size();
};

SenderConnection::pointer ConnectionPool::Borrow(SenderMapType &conn_map,
                                                 const ClientID &client_id) {
  SenderConnection::pointer conn = conn_map[client_id].back();
  conn_map[client_id].pop_back();
  return conn;
}

void ConnectionPool::Return(SenderMapType &conn_map, const ClientID &client_id,
                            SenderConnection::pointer conn) {
  conn_map[client_id].push_back(conn);
}

ray::Status ConnectionPool::GetNewConnection(ConnectionType type,
                                             const ClientID &client_id,
                                             SuccessCallback success_callback,
                                             FailureCallback failure_callback) {
  ray::Status status = Status::OK();
  status = object_directory_->GetInformation(
      client_id,
      [this, type, success_callback, failure_callback](RemoteConnectionInfo info) {
        success_callback(CreateConnection(type, info));
      },
      [this, failure_callback](const Status &status) { failure_callback(); });
  return status;
};

SenderConnection::pointer ConnectionPool::CreateConnection(ConnectionType type,
                                                           RemoteConnectionInfo info) {
  SenderConnection::pointer conn =
      SenderConnection::Create(*connection_service_, info.client_id, info.ip, info.port);
  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = (type == ConnectionType::TRANSFER);
  auto message =
      CreateConnectClientMessage(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Send synchronously.
  RAY_CHECK_OK(conn->WriteMessage(OMMessageType_ConnectClient, fbb.GetSize(),
                                  fbb.GetBufferPointer()));
  // The connection is ready, return to caller.
  return conn;
};

}  // namespace ray
