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
#include "plasma/plasma.h"

#include "object_directory.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

// TODO(hme): document public API after refactor.
class ObjectStoreClient {
 public:
  // Encapsulates communication with the object store.
  ObjectStoreClient(boost::asio::io_service &io_service, std::string &store_socket_name);

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  void SubscribeObjAdded(std::function<void(const ray::ObjectID &)> callback);

  // Subscribe to notifications of objects deleted from local store.
  void SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback);

  // TODO(hme): There should be as many client connections as there are threads.
  // Two client connections are made to enable concurrent communication with the store.
  plasma::PlasmaClient &GetClient();
  plasma::PlasmaClient &GetClientOther();

  // Terminate this object.
  void Terminate();

 private:
  std::vector<std::function<void(const ray::ObjectID &)>> add_handlers_;
  std::vector<std::function<void(const ray::ObjectID &)>> rem_handlers_;

  plasma::PlasmaClient client_one_;
  plasma::PlasmaClient client_two_;
  int c_socket_;
  int64_t length_;
  std::vector<uint8_t> notification_;
  boost::asio::local::stream_protocol::socket socket_;

  // Async loop for handling object store notifications.
  void NotificationWait();
  void ProcessStoreLength(const boost::system::error_code &error);
  void ProcessStoreNotification(const boost::system::error_code &error);

  // Support for rebroadcasting object add/rem events.
  void ProcessStoreAdd(const ObjectID &object_id);
  void ProcessStoreRemove(const ObjectID &object_id);
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_STORE_CLIENT_H
