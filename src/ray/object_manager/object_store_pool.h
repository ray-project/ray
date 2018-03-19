#ifndef RAY_OBJECT_STORE_POOL_H
#define RAY_OBJECT_STORE_POOL_H

#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"
#include "plasma/events.h"
#include "plasma/plasma.h"

#include "object_directory.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

class ObjectStorePool {
 public:
  std::mutex pool_mutex;

  // Encapsulates communication with the object store.
  ObjectStorePool(std::string &store_socket_name) {
    store_socket_name_ = store_socket_name;
  }

  ObjectStorePool &operator=(const ObjectStorePool &o) {
    throw std::runtime_error("Can't copy ObjectStorePool.");
  }

  std::shared_ptr<plasma::PlasmaClient> GetObjectStore() {
    pool_mutex.lock();
    if (available_clients.empty()) {
      Add();
    }
    std::shared_ptr<plasma::PlasmaClient> client = available_clients.back();
    available_clients.pop_back();
    pool_mutex.unlock();
    return client;
  }

  void ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client) {
    pool_mutex.lock();
    available_clients.push_back(client);
    pool_mutex.unlock();
  }

  void Terminate() {
    for (auto client : clients) {
      ARROW_CHECK_OK(client->Disconnect());
    }
    available_clients.clear();
    clients.clear();
  }

 private:
  void Add() {
    clients.emplace_back(new plasma::PlasmaClient());
    ARROW_CHECK_OK(clients.back()->Connect(store_socket_name_.c_str(), "",
                                           PLASMA_DEFAULT_RELEASE_DELAY));
    available_clients.push_back(clients.back());
  }

  std::vector<std::shared_ptr<plasma::PlasmaClient>> available_clients;
  std::vector<std::shared_ptr<plasma::PlasmaClient>> clients;
  std::string store_socket_name_;
};
}

#endif  // RAY_OBJECT_STORE_POOL_H
