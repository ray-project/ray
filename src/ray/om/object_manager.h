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

#include "ray/raylet/client_connection.h"

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
                   const ClientID &client_id);

  ray::Status AddSock(TCPClientConnection::pointer sock){
    sockets_.push_back(sock);
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
  ClientID client_id_;
  // TODO(hme): maintain ref to io_service and gcs_client.
  // boost::asio::io_service io_service_;
  OMConfig config;
  std::shared_ptr<ObjectDirectoryInterface> od;
  std::unique_ptr<ObjectStoreClient> store_client_;
  std::shared_ptr<GcsClient> gcs_client;

  std::vector<Request> send_queue_;
  std::vector<Request> receive_queue_;

  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> message_send_connections_;
  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> transfer_send_connections_;

  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> message_receive_connections_;
  std::unordered_map<ray::ClientID,
                     TCPClientConnection::pointer,
                     ray::UniqueIDHasher> transfer_receive_connections_;

  std::vector<TCPClientConnection::pointer> sockets_;

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

  ray::Status GetMsgConnection(const ClientID &client_id,
                               std::function<void(TCPClientConnection::pointer)> callback){
    if(message_send_connections_.count(client_id) > 0){
      callback(message_send_connections_[client_id]);
    } else {
      CreateMsgConnection(client_id, callback);
    }
    return Status::OK();
  };

  ray::Status CreateMsgConnection(const ClientID &client_id,
                                  std::function<void(TCPClientConnection::pointer)> callback){
    // TODO(hme): need gcs here...
//    boost::asio::ip::tcp::endpoint endpoint(
//        boost::asio::ip::address::from_string("127.0.0.1"),
//        40749);
//    boost::asio::ip::tcp::socket socket(io_service_);
//    socket.connect(endpoint);

    // NS handles this
    // 1. establish tcp connection

    // OM handles this
    // 2. send ClientID
    // 3. msg sock (flag)
    return Status::OK();
  };

  ray::Status GetTransferConnection(const ClientID &client_id,
                                    std::function<void(TCPClientConnection::pointer)> callback) {
    if (transfer_send_connections_.count(client_id) > 0) {
      callback(transfer_send_connections_[client_id]);
    } else {
      CreateTransferConnection(client_id, callback);
    }
    return Status::OK();
  };

  ray::Status CreateTransferConnection(const ClientID &client_id,
                                       std::function<void(TCPClientConnection::pointer)> callback){
    // need gcs here...
    return Status::OK();
  };

};

} // end namespace

#endif // RAY_OBJECTMANAGER_H
