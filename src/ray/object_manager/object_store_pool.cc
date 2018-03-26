#include "object_store_pool.h"

namespace ray {

ObjectStorePool::ObjectStorePool(std::string &store_socket_name) {
  store_socket_name_ = store_socket_name;
}

std::shared_ptr<plasma::PlasmaClient> ObjectStorePool::GetObjectStore() {
  pool_mutex.lock();
  if (available_clients.empty()) {
    Add();
  }
  std::shared_ptr<plasma::PlasmaClient> client = available_clients.back();
  available_clients.pop_back();
  pool_mutex.unlock();
  return client;
}

void ObjectStorePool::ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client) {
  pool_mutex.lock();
  available_clients.push_back(client);
  pool_mutex.unlock();
}

void ObjectStorePool::Terminate() {
  for (auto client : clients) {
    ARROW_CHECK_OK(client->Disconnect());
  }
  available_clients.clear();
  clients.clear();
}

void ObjectStorePool::Add() {
  clients.emplace_back(new plasma::PlasmaClient());
  ARROW_CHECK_OK(clients.back()->Connect(store_socket_name_.c_str(), "",
                                         PLASMA_DEFAULT_RELEASE_DELAY));
  available_clients.push_back(clients.back());
}
}  // namespace ray
