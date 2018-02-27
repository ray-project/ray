#ifndef RAY_STOREMESSENGER_H
#define RAY_STOREMESSENGER_H

#include <memory>
#include <vector>
#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/client.h"

#include "ray/id.h"
#include "ray/status.h"
#include "object_directory.h"

namespace ray {

class ObjectStoreClient {

 public:

  ObjectStoreClient(boost::asio::io_service &io_service,
                    std::string &store_socket_name,
                    std::shared_ptr<ObjectDirectoryInterface> od);

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  void SubscribeObjAdded(std::function<void(const ray::ObjectID&)> callback);

  // Subscribe to notifications of objects deleted from local store.
  void SubscribeObjDeleted(std::function<void(const ray::ObjectID&)> callback);

  plasma::PlasmaClient &GetClient();
  plasma::PlasmaClient &GetClientOther();

  void SetClientID(ClientID client_id);

  void Terminate();

 private:
  // async callback chain...
  void NotificationWait();
  void ProcessStoreLength(const boost::system::error_code &error);
  void ProcessStoreNotification(const boost::system::error_code &error);

  // Support for rebroadcasting object add/rem events.
  std::vector<std::function<void(const ray::ObjectID&)>> add_handlers;
  std::vector<std::function<void(const ray::ObjectID&)>> rem_handlers;
  void ProcessStoreAdd(const ObjectID& object_id);
  void ProcessStoreRemove(const ObjectID& object_id);

  plasma::PlasmaClient client_one_;
  plasma::PlasmaClient client_two_;
  int c_socket_;
  int64_t length_;
  std::vector<uint8_t> notification_;
  boost::asio::local::stream_protocol::socket socket_;
  std::shared_ptr<ObjectDirectoryInterface> od_;
  ClientID client_id_;
};

}

#endif //RAY_STOREMESSENGER_H
