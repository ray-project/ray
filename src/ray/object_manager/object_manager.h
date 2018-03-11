#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"
#include "plasma/events.h"
#include "plasma/plasma.h"

#include "format/object_manager_generated.h"
#include "object_directory.h"
#include "object_manager_client_connection.h"
#include "object_store_client.h"
#include "ray/id.h"
#include "ray/status.h"

namespace ray {

struct ObjectManagerConfig {
  // The time in milliseconds to wait before retrying a pull
  // that failed due to client id lookup.
  int pull_timeout_ms = 100;
  // TODO(hme): Implement num retries (to avoid infinite retries).
  std::string store_socket_name;
};

// TODO(hme): Comment everything doxygen-style.
// TODO(hme): Implement connection cleanup.
// TODO(hme): Add success/failure callbacks for push and pull.
// TODO(hme): Use boost thread pool.
// TODO(hme): Add incoming connections to io_service tied to thread pool.
class ObjectManager {
 public:
  /// Implicitly instantiates Ray implementation of ObjectDirectory.
  ///
  /// \param io_service The asio io_service tied to the object manager.
  /// \param config ObjectManager configuration.
  /// \param gcs_client A client connection to the Ray GCS.
  explicit ObjectManager(boost::asio::io_service &io_service, ObjectManagerConfig config,
                         std::shared_ptr<ray::GcsClient> gcs_client);

  /// Takes user-defined ObjectDirectoryInterface implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param io_service The asio io_service tied to the object manager.
  /// \param config ObjectManager configuration.
  /// \param od An object implementing the object directory interface.
  explicit ObjectManager(boost::asio::io_service &io_service, ObjectManagerConfig config,
                         std::unique_ptr<ObjectDirectoryInterface> od);

  /// \param client_id Set the client id associated with this node.
  void SetClientID(const ClientID &client_id);

  /// \return Get the client id associated with this node.
  ClientID GetClientID();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  ///
  /// already exist in the local store.
  /// \param callback The callback to invoke when objects are added to the local store.
  /// \return Status of whether adding the subscription succeeded.
  ray::Status SubscribeObjAdded(std::function<void(const ray::ObjectID &)> callback);

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback The callback to invoke when objects are removed from the local
  /// store.
  /// \return Status of whether adding the subscription succeeded.
  ray::Status SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback);

  /// Push an object to to the node manager on the node corresponding to client id.
  ///
  /// \param object_id The object's object id.
  /// \param client_id The remote node's client id.
  /// \return Status of whether the push request successfully initiated.
  ray::Status Push(const ObjectID &object_id, const ClientID &client_id);

  /// Pull an object from ClientID. Returns UniqueID asociated with
  /// an invocation of this method.
  ///
  /// \param object_id The object's object id.
  /// \return Status of whether the pull request successfully initiated.
  ray::Status Pull(const ObjectID &object_id);

  /// Discover ClientID via ObjectDirectory, then pull object
  /// from ClientID associated with ObjectID.
  ///
  /// \param object_id The object's object id.
  /// \param client_id The remote node's client id.
  /// \return Status of whether the pull request successfully initiated.
  ray::Status Pull(const ObjectID &object_id, const ClientID &client_id);

  /// Add a connection to a remote object manager.
  /// This is invoked by an external server.
  ///
  /// \param conn The connection.
  /// \return Status of whether the connection was successfully established.
  ray::Status AcceptConnection(TCPClientConnection::pointer conn);

  /// Cancels all requests (Push/Pull) associated with the given ObjectID.
  ///
  /// \param object_id The ObjectID.
  /// \return Status of whether requests were successfully cancelled.
  ray::Status Cancel(const ObjectID &object_id);

  // Callback definition for wait.
  using WaitCallback = std::function<void(const ray::Status, uint64_t,
                                          const std::vector<ray::ObjectID> &)>;
  /// Wait for timeout_ms before invoking the provided callback.
  /// If num_ready_objects is satisfied before the timeout, then
  /// invoke the callback.
  ///
  /// \param object_ids The object ids to wait on.
  /// \param timeout_ms The time in milliseconds to wait before invoking the callback.
  /// \param num_ready_objects The minimum number of objects required before
  /// invoking the callback.
  /// \param callback Invoked when either timeout_ms is satisfied OR num_ready_objects
  /// is satisfied.
  /// \return Status of whether the wait successfully initiated.
  ray::Status Wait(const std::vector<ObjectID> &object_ids, uint64_t timeout_ms,
                   int num_ready_objects, const WaitCallback &callback);

  /// \return Whether this object was successfully terminated.
  ray::Status Terminate();

 private:
  using BoostEC = const boost::system::error_code &;

  ClientID client_id_;
  ObjectManagerConfig config_;
  std::unique_ptr<ObjectDirectoryInterface> object_directory_;
  std::unique_ptr<ObjectStoreClient> store_client_;

  /// An io service for creating connections to other object managers.
  boost::asio::io_service io_service_;

  /// Used to create "work" for an io service, so when it's run, it doesn't exit.
  boost::asio::io_service::work work_;

  /// Single thread for executing asynchronous handlers.
  /// This runs the (currently only) io_service, which handles all outgoing requests
  /// and object transfers (push).
  std::thread io_thread_;

  /// Relatively simple way to add thread pooling.
  /// boost::thread_group thread_group_;

  /// Timeout for failed pull requests.
  using Timer = std::shared_ptr<boost::asio::deadline_timer>;
  std::unordered_map<ObjectID, Timer, UniqueIDHasher> pull_requests_;

  // TODO (hme): This needs to account for receives as well.
  /// This number is incremented whenever a push is started.
  int num_transfers_ = 0;
  // TODO (hme): Allow for concurrent sends.
  /// This is the maximum number of pushes allowed.
  /// We can only increase this number if we increase the number of
  /// plasma client connections.
  int max_transfers_ = 1;

  /// Note that (currently) receives take place on the main thread,
  /// and sends take place on a dedicated thread.
  std::unordered_map<ray::ClientID, SenderConnection::pointer, ray::UniqueIDHasher>
      message_send_connections_;
  std::unordered_map<ray::ClientID, SenderConnection::pointer, ray::UniqueIDHasher>
      transfer_send_connections_;

  std::unordered_map<ray::ClientID, TCPClientConnection::pointer, ray::UniqueIDHasher>
      message_receive_connections_;
  std::unordered_map<ray::ClientID, TCPClientConnection::pointer, ray::UniqueIDHasher>
      transfer_receive_connections_;

  /// Handle starting, running, and stopping asio io_service.
  void StartIOService();
  void IOServiceLoop();
  void StopIOService();

  /// Wait wait_ms milliseconds before triggering a pull request for object_id.
  /// This is invoked when a pull fails. Only point of failure currently considered
  /// is GetLocationsFailed.
  void SchedulePull(const ObjectID &object_id, int wait_ms);

  /// The handler for SchedulePull. Invokes a pull and removes the deadline timer
  /// that was added to schedule the pull.
  ray::Status SchedulePullHandler(const ObjectID &object_id);

  /// Synchronously send a pull request.
  /// Invoked once a connection to a remote manager that contains the required ObjectID
  /// is established.
  ray::Status ExecutePull(const ObjectID &object_id, SenderConnection::pointer conn);

  /// Invoked once a connection to the remote manager to which the ObjectID
  /// is to be sent is established.
  ray::Status QueuePush(const ObjectID &object_id, SenderConnection::pointer client);
  /// Starts as many queued pushes as possible without exceeding max_transfers_
  /// concurrent transfers.
  ray::Status ExecutePushQueue(SenderConnection::pointer client);
  /// Initiate a push. This method asynchronously sends the object id and object size
  /// to the remote object manager.
  ray::Status ExecutePushHeaders(const ObjectID &object_id,
                                 SenderConnection::pointer client);
  /// Called by the handler for ExecutePushMeta.
  /// This method initiates the actual object transfer.
  void ExecutePushObject(SenderConnection::pointer conn, const ObjectID &object_id,
                         const boost::system::error_code &header_ec);
  /// Invoked when a push is completed. This method will decrement num_transfers_
  /// and invoke ExecutePushQueue.
  ray::Status ExecutePushCompleted(const ObjectID &object_id,
                                   SenderConnection::pointer client);

  /// Private callback implementation for success on get location. Called inside OD.
  void GetLocationsSuccess(const std::vector<RemoteConnectionInfo> &vec,
                           const ObjectID &object_id);

  /// Private callback implementation for failure on get location. Called inside OD.
  void GetLocationsFailed(ray::Status status, const ObjectID &object_id);

  /// Asynchronously obtain a connection to client_id.
  /// If a connection to client_id already exists, the callback is invoked immediately.
  ray::Status GetMsgConnection(const ClientID &client_id,
                               std::function<void(SenderConnection::pointer)> callback);
  /// Asynchronously create a connection to client_id.
  ray::Status CreateMsgConnection(
      const RemoteConnectionInfo &info,
      std::function<void(SenderConnection::pointer)> callback);
  /// Asynchronously create a connection to client_id.
  ray::Status GetTransferConnection(
      const ClientID &client_id, std::function<void(SenderConnection::pointer)> callback);
  /// Asynchronously obtain a connection to client_id.
  /// If a connection to client_id already exists, the callback is invoked immediately.
  ray::Status CreateTransferConnection(
      const RemoteConnectionInfo &info,
      std::function<void(SenderConnection::pointer)> callback);

  /// A socket connection doing an asynchronous read on a transfer connection that was
  /// added by AcceptConnection.
  ray::Status WaitPushReceive(TCPClientConnection::pointer conn);
  /// Invoked when a remote object manager pushes an object to this object manager.
  void HandlePushReceive(TCPClientConnection::pointer conn, BoostEC length_ec);

  /// A socket connection doing an asynchronous read on a message connection that was
  /// added by AcceptConnection.
  ray::Status WaitMessage(TCPClientConnection::pointer conn);
  /// Handle messages.
  void HandleMessage(TCPClientConnection::pointer conn, BoostEC msg_ec);
  /// Process the receive pull request message.
  void ReceivePullRequest(TCPClientConnection::pointer conn);

  /// Register object add with directory.
  void NotifyDirectoryObjectAdd(const ObjectID &object_id);
  /// Register object remove with directory.
  void NotifyDirectoryObjectDeleted(const ObjectID &object_id);
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
