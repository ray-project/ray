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

#include "ray/common/client_connection.h"
#include "ray/id.h"
#include "ray/status.h"

#include "plasma/client.h"
#include "plasma/events.h"
#include "plasma/plasma.h"

#include "connection_pool.h"
#include "format/object_manager_generated.h"
#include "object_directory.h"
#include "object_manager_client_connection.h"
#include "object_store_notification.h"
#include "object_store_pool.h"
#include "transfer_queue.h"

namespace ray {

struct ObjectManagerConfig {
  // The time in milliseconds to wait before retrying a pull
  // that failed due to client id lookup.
  int pull_timeout_ms = 100;
  // TODO(hme): Implement num retries (to avoid infinite retries).
  std::string store_socket_name;
};

// TODO(hme): Implement connection cleanup.
// TODO(hme): Add success/failure callbacks for push and pull.
// TODO(hme): Use boost thread pool.
class ObjectManager {
 public:
  /// Implicitly instantiates Ray implementation of ObjectDirectory.
  ///
  /// \param main_service The main asio io_service.
  /// \param object_manager_service The asio io_service tied to the object manager.
  /// \param config ObjectManager configuration.
  /// \param gcs_client A client connection to the Ray GCS.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         std::unique_ptr<boost::asio::io_service> object_manager_service,
                         ObjectManagerConfig config,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client);

  /// Takes user-defined ObjectDirectoryInterface implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param main_service The main asio io_service.
  /// \param object_manager_service The asio io_service tied to the object manager.
  /// \param config ObjectManager configuration.
  /// \param od An object implementing the object directory interface.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         std::unique_ptr<boost::asio::io_service> object_manager_service,
                         ObjectManagerConfig config,
                         std::unique_ptr<ObjectDirectoryInterface> od);

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
  void ProcessNewClient(std::shared_ptr<ReceiverConnection> conn);

  /// Process messages sent from other nodes. We only establish
  /// transfer connections using this method; all other transfer communication
  /// is done separately.
  ///
  /// \param conn The connection.
  /// \param message_type The message type.
  /// \param message A pointer set to the beginning of the message.
  void ProcessClientMessage(std::shared_ptr<ReceiverConnection> conn,
                            int64_t message_type, const uint8_t *message);

  /// Cancels all requests (Push/Pull) associated with the given ObjectID.
  ///
  /// \param object_id The ObjectID.
  /// \return Status of whether requests were successfully cancelled.
  ray::Status Cancel(const ObjectID &object_id);

  /// Callback definition for wait.
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
  ClientID client_id_;
  ObjectManagerConfig config_;
  std::unique_ptr<ObjectDirectoryInterface> object_directory_;
  std::unique_ptr<ObjectStoreNotification> store_notification_;
  std::unique_ptr<ObjectStorePool> store_pool_;

  /// An io service for creating connections to other object managers.
  std::unique_ptr<boost::asio::io_service> object_manager_service_;
  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  boost::asio::io_service *main_service_;

  /// Used to create "work" for an io service, so when it's run, it doesn't exit.
  boost::asio::io_service::work work_;

  /// Single thread for executing asynchronous handlers.
  /// This runs the (currently only) io_service, which handles
  /// all outgoing requests and object transfers (push).
  std::thread io_thread_;

  /// Connection pool provides connections to other object managers.
  ConnectionPool connection_pool_;

  /// Relatively simple way to add thread pooling.
  // boost::thread_group thread_group_;

  /// Timeout for failed pull requests.
  using Timer = std::shared_ptr<boost::asio::deadline_timer>;
  std::unordered_map<ObjectID, Timer, UniqueIDHasher> pull_requests_;

  /// This number is incremented whenever a push is started.
  int num_transfers_ = 0;
  /// This is the maximum number of pushes allowed.
  /// We can only increase this number if we increase the number of
  /// plasma client connections.
  int max_transfers_ = 1;

  /// Allows control of concurrent object transfers. This is a global queue,
  /// allowing for concurrent transfers with many object managers as well as
  /// concurrent transfers, including both sends and receives, with a single
  /// remote object manager.
  TransferQueue transfer_queue_;

  /// Read length for push receives.
  uint64_t read_length_;

  /// Cache of locally available objects.
  std::unordered_set<ObjectID, UniqueIDHasher> local_objects_;

  /// Handle starting, running, and stopping asio io_service.
  void StartIOService();
  void IOServiceLoop();
  void StopIOService();

  /// Register object add with directory.
  void NotifyDirectoryObjectAdd(const ObjectID &object_id);

  /// Register object remove with directory.
  void NotifyDirectoryObjectDeleted(const ObjectID &object_id);

  /// Wait wait_ms milliseconds before triggering a pull request for object_id.
  /// This is invoked when a pull fails. Only point of failure currently considered
  /// is GetLocationsFailed.
  void SchedulePull(const ObjectID &object_id, int wait_ms);

  /// Invokes a pull and removes the deadline timer
  /// that was added to schedule the pull.
  /// Guaranteed to execute on main_service_ thread.
  ray::Status Pull_(const ObjectID &object_id);

  /// Private callback implementation for success on get location. Called inside OD.
  void GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                           const ray::ObjectID &object_id);

  /// Private callback implementation for failure on get location. Called inside OD.
  void GetLocationsFailed(ray::Status status, const ObjectID &object_id);

  /// Synchronously send a pull request.
  /// Invoked once a connection to a remote manager that contains the required ObjectID
  /// is established.
  ray::Status ExecutePull(const ObjectID &object_id, SenderConnection::pointer conn);

  /// Guaranteed to execute on main_service_ thread.
  ray::Status Push_(const ObjectID &object_id, const ClientID &client_id);

  /// Starts as many queued transfers as possible without exceeding max_transfers_
  /// concurrent transfers. Alternates between queued sends and receives.
  ray::Status DequeueTransfers();

  /// Invoked when a transfer is completed. This method will decrement num_transfers_
  /// and invoke DequeueTransfers.
  ray::Status TransferCompleted();

  /// Begin executing a send.
  ray::Status ExecuteSend(const ObjectID &object_id, const ClientID &client_id);
  /// Initiate a push. This method asynchronously sends the object id and object size
  /// to the remote object manager.
  ray::Status SendHeaders(const ObjectID &object_id, SenderConnection::pointer client);
  /// Called by the handler for ExecutePushMeta.
  /// This method initiates the actual object transfer.
  void SendObject(SenderConnection::pointer conn, const UniqueID &context_id,
                  std::shared_ptr<plasma::PlasmaClient> store_client,
                  const boost::system::error_code &header_ec);

  /// A socket connection doing an asynchronous read on a transfer connection that was
  /// added by ConnectClient.
  ray::Status WaitPushReceive(std::shared_ptr<ReceiverConnection> conn);
  /// Invoked when a remote object manager pushes an object to this object manager.
  /// This will queue the receive.
  void HandlePushReceive(std::shared_ptr<ReceiverConnection> conn,
                         const boost::system::error_code &length_ec);
  /// Execute a receive that was in the queue.
  ray::Status ExecuteReceive(ClientID client_id, ObjectID object_id, uint64_t object_size,
                             std::shared_ptr<ReceiverConnection> conn);

  /// Handles receiving a pull request message.
  void ReceivePullRequest(std::shared_ptr<ReceiverConnection> &conn,
                          const uint8_t *message);
  /// Handles connect message of a new client connection.
  void ConnectClient(std::shared_ptr<ReceiverConnection> &conn, const uint8_t *message);
  /// Handles disconnect message of an existing client connection.
  void DisconnectClient(std::shared_ptr<ReceiverConnection> &conn,
                        const uint8_t *message);
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
