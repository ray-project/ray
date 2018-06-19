#include "ray/object_manager/connection_pool.h"

namespace ray {

ConnectionPool::ConnectionPool(uint32_t max_sender_connection)
    : max_sender_connection_count(max_sender_connection) {}

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

void ConnectionPool::RemoveReceiver(std::shared_ptr<TcpClientConnection> conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  ClientID client_id = conn->GetClientID();
  if (message_receive_connections_.count(client_id) != 0) {
    Remove(message_receive_connections_, client_id, conn);
  }
  if (transfer_receive_connections_.count(client_id) != 0) {
    Remove(transfer_receive_connections_, client_id, conn);
  }
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
                                          std::shared_ptr<SenderConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? available_message_send_connections_
                                : available_transfer_send_connections_;
  Return(conn_map, conn->GetClientID(), conn);
  return ray::Status::OK();
}

void ConnectionPool::Add(ReceiverMapType &conn_map, const ClientID &client_id,
                         std::shared_ptr<TcpClientConnection> conn) {
  conn_map[client_id].push_back(std::move(conn));
}

void ConnectionPool::Add(SenderMapType &conn_map, const ClientID &client_id,
                         std::shared_ptr<SenderConnection> conn) {
  conn_map[client_id].push_back(std::move(conn));
}

void ConnectionPool::Remove(ReceiverMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<TcpClientConnection> &conn) {
  auto it = conn_map.find(client_id);
  if (it == conn_map.end()) {
    return;
  }
  auto &connections = it->second;
  int64_t pos =
      std::find(connections.begin(), connections.end(), conn) - connections.begin();
  if (pos >= (int64_t)connections.size()) {
    return;
  }
  connections.erase(connections.begin() + pos);
}

uint64_t ConnectionPool::Count(SenderMapType &conn_map, const ClientID &client_id) {
  auto it = conn_map.find(client_id);
  if (it == conn_map.end()) {
    return 0;
  }
  return it->second.size();
}

std::shared_ptr<SenderConnection> ConnectionPool::Borrow(SenderMapType &conn_map,
                                                         const ClientID &client_id) {
  std::shared_ptr<SenderConnection> conn = std::move(conn_map[client_id].back());
  conn_map[client_id].pop_back();
  RAY_LOG(DEBUG) << "Borrow " << client_id << " " << conn_map[client_id].size();
  return conn;
}

void ConnectionPool::Return(SenderMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<SenderConnection> conn) {
  conn_map[client_id].push_back(std::move(conn));
  RAY_LOG(DEBUG) << "Return " << client_id << " " << conn_map[client_id].size();
}

bool ConnectionPool::CanAddSender(ConnectionType type, const ClientID &client_id) {
  if (max_sender_connection_count == 0) {
    // There is no limit on the connection.
    return true;
  }
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? message_send_connections_
                                : transfer_send_connections_;
  std::unique_lock<std::mutex> guard(connection_mutex);
  return Count(conn_map, client_id) < max_sender_connection_count;
}

}  // namespace ray
