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
  /// Provides connections to the object store. Enables concurrent communication with
  /// the object store.
  ///
  /// \param store_socket_name The object store socket name.
  ObjectStorePool(std::string &store_socket_name);

  /// This object cannot be copied due to pool_mutex.
  ObjectStorePool &operator=(const ObjectStorePool &o) {
    throw std::runtime_error("Can't copy ObjectStorePool.");
  }

  /// Provides a connection to the object store from the object store pool.
  /// This removes the object store client from the pool of available clients.
  ///
  /// \return A connection to the object store.
  std::shared_ptr<plasma::PlasmaClient> GetObjectStore();

  /// Returns a client to the object store pool.
  /// Once a client is released, it is assumed that it is not being used.
  /// \param client The client to return.
  /// \param client
  void ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client);

  /// Terminates this object.
  void Terminate();

 private:
  /// Adds a client to the client pool and mark it as available.
  void Add();

  std::mutex pool_mutex;
  std::vector<std::shared_ptr<plasma::PlasmaClient>> available_clients;
  std::vector<std::shared_ptr<plasma::PlasmaClient>> clients;
  std::string store_socket_name_;
};
}  // namespace ray

#endif  // RAY_OBJECT_STORE_POOL_H
