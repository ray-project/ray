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
  std::unique_lock<std::mutex> guard(connection_mutex);
  switch (type) {
  case MESSAGE: {
    Add(message_receive_connections_, client_id, conn);
  } break;
  case TRANSFER: {
    Add(transfer_receive_connections_, client_id, conn);
  } break;
  }
}

void ConnectionPool::RemoveReceiver(ConnectionType type, const ClientID &client_id,
                                    std::shared_ptr<ReceiverConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
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

ray::Status ConnectionPool::GetSender(ConnectionType type, ClientID client_id,
                                      SuccessCallback success_callback,
                                      FailureCallback failure_callback) {
  SenderMapType &avail_conn_map = (type == MESSAGE)
                                      ? available_message_send_connections_
                                      : available_transfer_send_connections_;
  connection_mutex.lock();
  if (Count(avail_conn_map, client_id) > 0) {
    SenderConnection::pointer conn = Borrow(avail_conn_map, client_id);
    connection_mutex.unlock();
    success_callback(conn);
    return Status::OK();
  }
  connection_mutex.unlock();

  return CreateConnection1(
      type, client_id,
      [this, type, client_id, success_callback](SenderConnection::pointer conn) {
        connection_mutex.lock();
        // add it to the connection map to maintain ownership.
        SenderMapType &conn_map =
            (type == MESSAGE) ? message_send_connections_ : transfer_send_connections_;
        Add(conn_map, client_id, conn);
        // add it to the available connections
        SenderMapType &avail_conn_map = (type == MESSAGE)
                                            ? available_message_send_connections_
                                            : available_transfer_send_connections_;
        Add(avail_conn_map, client_id, conn);
        // "borrow" the connection.
        SenderConnection::pointer async_conn = Borrow(avail_conn_map, client_id);
        connection_mutex.unlock();
        success_callback(async_conn);
      },
      failure_callback);
}

ray::Status ConnectionPool::ReleaseSender(ConnectionType type,
                                          SenderConnection::pointer conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &conn_map = (type == MESSAGE) ? available_message_send_connections_
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
//  RAY_LOG(INFO) << "Borrow "
//                << client_id_ << " "
//                << client_id << " "
//                << conn_map[client_id].size();
  return conn;
}

void ConnectionPool::Return(SenderMapType &conn_map, const ClientID &client_id,
                            SenderConnection::pointer conn) {
  conn_map[client_id].push_back(conn);
//  RAY_LOG(INFO) << "Return "
//      << client_id_ << " "
//      << client_id << " "
//      << conn_map[client_id].size();
}

/// Asynchronously obtain a connection to client_id.
/// If a connection to client_id already exists, the callback is invoked immediately.
ray::Status ConnectionPool::CreateConnection1(ConnectionType type,
                                              const ClientID &client_id,
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
ray::Status ConnectionPool::CreateConnection2(ConnectionType type,
                                              RemoteConnectionInfo info,
                                              SuccessCallback success_callback,
                                              FailureCallback failure_callback) {
  //    RAY_LOG(INFO) << "CreateConnection2";
  SenderConnection::pointer conn =
      SenderConnection::Create(*connection_service_, info.client_id, info.ip, info.port);
  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  bool is_transfer = (type == TRANSFER);
  auto message =
      CreateConnectClientMessage(fbb, fbb.CreateString(client_id_.binary()), is_transfer);
  fbb.Finish(message);
  // Send synchronously.
  (void)conn->WriteMessage(OMMessageType_ConnectClient, fbb.GetSize(),
                           fbb.GetBufferPointer());
  // The connection is ready, invoke callback with connection info.
  success_callback(conn);
  return Status::OK();
};

}  // namespace ray
