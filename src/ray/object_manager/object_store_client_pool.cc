#include "object_store_client_pool.h"

namespace ray {

ObjectStoreClientPool::ObjectStoreClientPool(const std::string &store_socket_name)
    : store_socket_name_(store_socket_name) {}

std::shared_ptr<plasma::PlasmaClient> ObjectStoreClientPool::GetObjectStore() {
  std::lock_guard<std::mutex> lock(pool_mutex);
  if (available_clients.empty()) {
    Add();
  }
  std::shared_ptr<plasma::PlasmaClient> client = available_clients.back();
  available_clients.pop_back();
  return client;
}

void ObjectStoreClientPool::ReleaseObjectStore(
    std::shared_ptr<plasma::PlasmaClient> client) {
  std::lock_guard<std::mutex> lock(pool_mutex);
  available_clients.push_back(client);
}

void ObjectStoreClientPool::Terminate() {
  for (const auto &client : clients) {
    ARROW_CHECK_OK(client->Disconnect());
  }
  available_clients.clear();
  clients.clear();
}

void ObjectStoreClientPool::Add() {
  clients.emplace_back(new plasma::PlasmaClient());
  ARROW_CHECK_OK(clients.back()->Connect(store_socket_name_.c_str(), "",
                                         PLASMA_DEFAULT_RELEASE_DELAY));
  available_clients.push_back(clients.back());
}
}  // namespace ray
