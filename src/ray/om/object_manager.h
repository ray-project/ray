#ifndef RAY_OBJECTMANAGER_H
#define RAY_OBJECTMANAGER_H

#include <memory>
#include <cstdint>
#include <vector>
#include <map>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/client.h"

#include "ray/id.h"
#include "ray/status.h"

#include "ray/om/object_directory.h"
#include "ray/om/object_store_client.h"

namespace ray {

struct OMConfig {
  int num_retries = 5;
  std::string store_socket_name;
};

struct Request {
  ObjectID object_id;
  ClientID client_id;
};

struct Connection {

};

class ObjectManager {

 public:

  // Callback signatures for Push and Pull. Please keep until we're certain
  // they will not be necessary (hme).
  using TransferCallback = std::function<void(ray::Status,
                                         const ray::ObjectID&,
                                         const ray::ClientID&)>;

  using WaitCallback = std::function<void(const ray::Status,
                                          uint64_t,
                                          const std::vector<ray::ObjectID>&)>;

  // Instantiates Ray implementation of ObjectDirectory.
  explicit ObjectManager(boost::asio::io_service &io_service,
                         OMConfig config);

  // Takes user-defined ObjectDirectoryInterface implementation.
  // When this constructor is used, the ObjectManager assumes ownership of
  // the given ObjectDirectory instance.
  explicit ObjectManager(boost::asio::io_service &io_service,
                         OMConfig config,
                         std::shared_ptr<ObjectDirectoryInterface> od);

  // Subscribe to notifications of objects added to local store.
  // Upon subscribing, the callback will be invoked for all objects that
  // already exist in the local store.
  ray::Status SubscribeObjAdded(std::function<void(const ray::ObjectID&)> callback);

  // Subscribe to notifications of objects deleted from local store.
  ray::Status SubscribeObjDeleted(std::function<void(const ray::ObjectID&)> callback);

  // Push an object to DBClientID.
  ray::Status Push(const ObjectID &object_id,
                   const ClientID &dbclient_id);

  // Pull an object from DBClientID. Returns UniqueID associated with
  // an invocation of this method.
  ray::Status Pull(const ObjectID &object_id);

  // Discover DBClientID via ObjectDirectory, then pull object
  // from DBClientID associated with ObjectID.
  ray::Status Pull(const ObjectID &object_id,
                   const ClientID &dbclient_id);

  ray::Status AddSock(const ClientID &client_id,
                      const boost::asio::ip::tcp::socket &sock){
    // 1. read ClientID
    // 2. read sock type
    return ray::Status::OK();
  };

  // Cancels all requests (Push/Pull) associated with the given ObjectID.
  ray::Status Cancel(const ObjectID &object_id);

  // Wait for timeout_ms before invoking the provided callback.
  // If num_ready_objects is satisfied before the timeout, then
  // invoke the callback.
  ray::Status Wait(const std::vector<ObjectID> &object_ids,
                   uint64_t timeout_ms,
                   int num_ready_objects,
                   const WaitCallback &callback);

  ray::Status Terminate();

 private:
  OMConfig config;
  std::shared_ptr<ObjectDirectoryInterface> od;
  std::unique_ptr<ObjectStoreClient> store_client;

  std::vector<Request> send_queue;
  std::vector<Request> receive_queue;

  ray::Status ExecutePull(const ObjectID &object_id,
                          const ClientID &dbclient_id);

  ray::Status ExecutePush(const ObjectID &object_id,
                          const ClientID &dbclient_id);

  /// callback that gets called internally to OD on get location success.
  void GetLocationsSuccess(const std::vector<ODRemoteConnectionInfo>& v,
                           const ObjectID &object_id);

  /// callback that gets called internally to OD on get location failure.
   void GetLocationsFailed(ray::Status status,
                           const ObjectID &object_id);

  void GetMsgConnection(const ClientID &client_id){

  };

  void CreateMsgConnection(){
    // NS handles this
    // 1. establish tcp connection

    // OM handles this
    // 2. send ClientID
    // 3. msg sock (flag)
  }

  void GetTransferConnection(const ClientID &client_id){

  };

  void CreateTransferConnection(){

  }

};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
