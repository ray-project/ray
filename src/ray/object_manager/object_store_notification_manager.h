#ifndef RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
#define RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H

#include <list>
#include <memory>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"
#include "plasma/events.h"

#include "ray/id.h"
#include "ray/status.h"

#include "ray/object_manager/object_directory.h"

namespace ray {

/// \class ObjectStoreClientPool
///
/// Encapsulates notification handling from the object store.
class ObjectStoreNotificationManager {
 public:
  /// Constructor.
  ///
  /// \param io_service The asio service to be used.
  /// \param store_socket_name The store socket to connect to.
  ObjectStoreNotificationManager(boost::asio::io_service &io_service,
                                 const std::string &store_socket_name);

  ~ObjectStoreNotificationManager();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  /// already exist in the local store
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjAdded(std::function<void(const object_manager::protocol::ObjectInfoT &)> callback);

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback A callback expecting an ObjectID.
  void SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback);

 private:
  /// Async loop for handling object store notifications.
  void NotificationWait();
  void ProcessStoreLength(const boost::system::error_code &error);
  void ProcessStoreNotification(const boost::system::error_code &error);

  /// Support for rebroadcasting object add/rem events.
  void ProcessStoreAdd(const object_manager::protocol::ObjectInfoT &object_info);
  void ProcessStoreRemove(const ObjectID &object_id);

  std::vector<std::function<void(const object_manager::protocol::ObjectInfoT &)>> add_handlers_;
  std::vector<std::function<void(const ray::ObjectID &)>> rem_handlers_;

  plasma::PlasmaClient store_client_;
  int c_socket_;
  int64_t length_;
  std::vector<uint8_t> notification_;
  boost::asio::local::stream_protocol::socket socket_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
