#ifndef RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_POOL_H
#define RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_POOL_H

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

/// \class ObjectStoreClientPool
///
/// Provides connections to the object store. Enables concurrent communication with
/// the object store.
class ObjectStoreClientPool {
 public:
  /// Constructor.
  ///
  /// \param store_socket_name The object store socket name.
  ObjectStoreClientPool(const std::string &store_socket_name);

  /// This object cannot be copied due to pool_mutex.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectStoreClientPool);

  /// Provides a connection to the object store from the object store pool.
  /// This removes the object store client from the pool of available clients.
  ///
  /// \return A connection to the object store.
  std::shared_ptr<plasma::PlasmaClient> GetObjectStore();

  /// Releases a client object and puts it back into the object store pool
  /// for reuse.
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

#endif  // RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_POOL_H
