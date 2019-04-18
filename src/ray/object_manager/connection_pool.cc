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

void ConnectionPool::RemoveReceiver(std::shared_ptr<TcpClientConnection> conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  const ClientID client_id = conn->GetClientId();
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

void ConnectionPool::RemoveSender(const std::shared_ptr<SenderConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  const ClientID client_id = conn->GetClientId();
  if (message_send_connections_.count(client_id) != 0) {
    Remove(message_send_connections_, client_id, conn);
  }
  if (transfer_send_connections_.count(client_id) != 0) {
    Remove(transfer_send_connections_, client_id, conn);
  }
}

void ConnectionPool::GetSender(ConnectionType type, const ClientID &client_id,
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
}

void ConnectionPool::ReleaseSender(ConnectionType type,
                                   std::shared_ptr<SenderConnection> &conn) {
  std::unique_lock<std::mutex> guard(connection_mutex);
  SenderMapType &conn_map = (type == ConnectionType::MESSAGE)
                                ? available_message_send_connections_
                                : available_transfer_send_connections_;
  Return(conn_map, conn->GetClientId(), conn);
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
  if (pos >= static_cast<int64_t>(connections.size())) {
    return;
  }
  connections.erase(connections.begin() + pos);
}

void ConnectionPool::Remove(SenderMapType &conn_map, const ClientID &client_id,
                            const std::shared_ptr<SenderConnection> &conn) {
  auto it = conn_map.find(client_id);
  if (it == conn_map.end()) {
    return;
  }
  auto &connections = it->second;
  int64_t pos =
      std::find(connections.begin(), connections.end(), conn) - connections.begin();
  if (pos >= static_cast<int64_t>(connections.size())) {
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
  return conn;
}

void ConnectionPool::Return(SenderMapType &conn_map, const ClientID &client_id,
                            std::shared_ptr<SenderConnection> conn) {
  conn_map[client_id].push_back(std::move(conn));
}

std::string ConnectionPool::DebugString() const {
  std::stringstream result;
  result << "ConnectionPool:";
  result << "\n- num message send connections: " << message_send_connections_.size();
  result << "\n- num transfer send connections: " << transfer_send_connections_.size();
  result << "\n- num avail message send connections: "
         << available_transfer_send_connections_.size();
  result << "\n- num avail transfer send connections: "
         << available_transfer_send_connections_.size();
  result << "\n- num message receive connections: "
         << message_receive_connections_.size();
  result << "\n- num transfer receive connections: "
         << transfer_receive_connections_.size();
  return result.str();
}

void ConnectionPool::RecordMetrics() const {
  stats::ConnectionPoolStats().Record(
      message_send_connections_.size(),
      {{stats::ValueTypeKey, "num_message_send_connections"}});
  stats::ConnectionPoolStats().Record(
      transfer_send_connections_.size(),
      {{stats::ValueTypeKey, "num_transfer_send_connections"}});
  stats::ConnectionPoolStats().Record(
      available_transfer_send_connections_.size(),
      {{stats::ValueTypeKey, "num_avail_message_send_connections"}});
  stats::ConnectionPoolStats().Record(
      available_transfer_send_connections_.size(),
      {{stats::ValueTypeKey, "num_avail_transfer_send_connections"}});
  stats::ConnectionPoolStats().Record(
      message_receive_connections_.size(),
      {{stats::ValueTypeKey, "num_message_receive_connections"}});
  stats::ConnectionPoolStats().Record(
      transfer_receive_connections_.size(),
      {{stats::ValueTypeKey, "num_transfer_receive_connections"}});
}

}  // namespace ray
