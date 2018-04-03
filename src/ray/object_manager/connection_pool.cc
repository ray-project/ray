#include "ray/object_manager/connection_pool.h"

namespace ray {

ConnectionPool::ConnectionPool() {}

void ConnectionPool::RegisterReceiver(ConnectionType type, const ClientID &client_id,
                                      std::shared_ptr<TcpClientConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
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
                                    std::shared_ptr<TcpClientConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
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

void ConnectionPool::RegisterSender(ConnectionType type, const ClientID &client_id,
                                    std::shared_ptr<SenderConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? message_send_connections_
                                : transfer_send_connections_;
  Add(conn_map, client_id, conn);
  // Don't add to available connections. It will become available once it is released.
}

ray::Status ConnectionPool::GetSender(ConnectionType type, const ClientID &client_id,
                                      std::shared_ptr<SenderConnection> *conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &avail_conn_map = (type == ConnectionType::MESSAGE)
                                      ? available_message_send_connections_
                                      : available_transfer_send_connections_;
  if (Count(avail_conn_map, client_id) > 0) {
    *conn = Borrow(avail_conn_map, client_id);
  } else {
    *conn = nullptr;
  }
  return ray::Status::OK();
}

ray::Status ConnectionPool::ReleaseSender(ConnectionType type,
                                          std::shared_ptr<SenderConnection> conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? available_message_send_connections_
                                : available_transfer_send_connections_;
  Return(conn_map, conn->GetClientID(), conn);
  return ray::Status::OK();
}

void ConnectionPool::Add(ReceiverMapType &conn_map, const ClientID &client_id,
                         std::shared_ptr<TcpClientConnection> conn) {
  conn_map[client_id].push_back(conn);
}

void ConnectionPool::Add(SenderMapType &conn_map, const ClientID &client_id,
                         std::shared_ptr<SenderConnection> conn) {
  conn_map[client_id].push_back(conn);
}

void ConnectionPool::Remove(ReceiverMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<TcpClientConnection> conn) {
  if (conn_map.count(client_id) == 0) {
    return;
  }
  std::vector<std::shared_ptr<TcpClientConnection>> &connections = conn_map[client_id];
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
}

std::shared_ptr<SenderConnection> ConnectionPool::Borrow(SenderMapType &conn_map,
                                                         const ClientID &client_id) {
  std::shared_ptr<SenderConnection> conn = conn_map[client_id].back();
  conn_map[client_id].pop_back();
  RAY_LOG(DEBUG) << "Borrow " << client_id << " " << conn_map[client_id].size();
  return conn;
}

void ConnectionPool::Return(SenderMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<SenderConnection> conn) {
  conn_map[client_id].push_back(conn);
  RAY_LOG(DEBUG) << "Return " << client_id << " " << conn_map[client_id].size();
}

}  // namespace ray
