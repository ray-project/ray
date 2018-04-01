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

#include "ray/object_manager/connection_pool.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/object_manager_client_connection.h"
#include "ray/object_manager/object_store_client_pool.h"
#include "ray/object_manager/object_store_notification_manager.h"
#include "ray/object_manager/transfer_queue.h"

namespace ray {

struct ObjectManagerConfig {
  /// The time in milliseconds to wait before retrying a pull
  /// that failed due to client id lookup.
  int pull_timeout_ms = 100;
  /// Size of thread pool.
  int num_threads = 2;
  /// Maximum number of sends allowed.
  int max_sends = 20;
  /// Maximum number of receives allowed.
  int max_receives = 20;
  // TODO(hme): Implement num retries (to avoid infinite retries).
  std::string store_socket_name;
};

// TODO(hme): Add success/failure callbacks for push and pull.
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
                         const ObjectManagerConfig &config,
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
                         const ObjectManagerConfig &config,
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
  void ProcessNewClient(std::shared_ptr<TcpClientConnection> conn);

  /// Process messages sent from other nodes. We only establish
  /// transfer connections using this method; all other transfer communication
  /// is done separately.
  ///
  /// \param conn The connection.
  /// \param message_type The message type.
  /// \param message A pointer set to the beginning of the message.
  void ProcessClientMessage(std::shared_ptr<TcpClientConnection> conn,
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
  ObjectStoreNotificationManager store_notification_;
  ObjectStoreClientPool store_pool_;

  /// An io service for creating connections to other object managers.
  /// This runs on a thread pool.
  std::unique_ptr<boost::asio::io_service> object_manager_service_;
  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  boost::asio::io_service *main_service_;

  /// Used to create "work" for an io service, so when it's run, it doesn't exit.
  boost::asio::io_service::work work_;

  /// Thread pool for executing asynchronous handlers.
  /// These run the object_manager_service_, which handle
  /// all incoming and outgoing object transfers.
  std::vector<std::thread> io_threads_;

  /// Connection pool for reusing outgoing connections to remote object managers.
  ConnectionPool connection_pool_;

  /// Timeout for failed pull requests.
  std::unordered_map<ObjectID, std::shared_ptr<boost::asio::deadline_timer>,
                     UniqueIDHasher>
      pull_requests_;

  /// Allows control of concurrent object transfers. This is a global queue,
  /// allowing for concurrent transfers with many object managers as well as
  /// concurrent transfers, including both sends and receives, with a single
  /// remote object manager.
  TransferQueue transfer_queue_;

  /// Variables to track number of concurrent sends and receives.
  std::atomic<int> num_transfers_send_;
  std::atomic<int> num_transfers_receive_;

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

  /// Part of an asynchronous sequence of Pull methods.
  /// Gets the location of an object before invoking PullEstablishConnection.
  /// Guaranteed to execute on main_service_ thread.
  /// Executes on main_service_ thread.
  ray::Status PullGetLocations(const ObjectID &object_id);

  /// Part of an asynchronous sequence of Pull methods.
  /// Uses an existing connection or creates a connection to ClientID.
  /// Executes on main_service_ thread.
  ray::Status PullEstablishConnection(const ObjectID &object_id,
                                      const ClientID &client_id);

  /// Private callback implementation for success on get location. Called from
  /// ObjectDirectory.
  void GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                           const ray::ObjectID &object_id);

  /// Private callback implementation for failure on get location. Called from
  /// ObjectDirectory.
  void GetLocationsFailed(const ObjectID &object_id);

  /// Synchronously send a pull request via remote object manager connection.
  /// Executes on main_service_ thread.
  ray::Status PullSendRequest(const ObjectID &object_id,
                              std::shared_ptr<SenderConnection> conn);

  /// Starts as many queued sends and receives as possible without exceeding
  /// config_.max_sends and config_.max_receives, respectively.
  /// Executes on object_manager_service_ thread pool.
  ray::Status DequeueTransfers();

  std::shared_ptr<SenderConnection> CreateSenderConnection(
      ConnectionPool::ConnectionType type, RemoteConnectionInfo info);

  /// Invoked when a transfer is completed. Invokes DequeueTransfers after
  /// updating variables that track concurrent transfers.
  /// Executes on object_manager_service_ thread pool.
  ray::Status TransferCompleted(TransferQueue::TransferType type);

  /// Begin executing a send.
  /// Executes on object_manager_service_ thread pool.
  ray::Status ExecuteSendObject(const ObjectID &object_id, const ClientID &client_id,
                                const RemoteConnectionInfo &connection_info);
  /// This method synchronously sends the object id and object size
  /// to the remote object manager.
  /// Executes on object_manager_service_ thread pool.
  ray::Status SendObjectHeaders(const ObjectID &object_id,
                                std::shared_ptr<SenderConnection> client);

  /// This method initiates the actual object transfer.
  /// Executes on object_manager_service_ thread pool.
  ray::Status SendObjectData(std::shared_ptr<SenderConnection> conn,
                             const UniqueID &context_id,
                             std::shared_ptr<plasma::PlasmaClient> store_client);

  /// Invoked when a remote object manager pushes an object to this object manager.
  /// This will queue the receive.
  void ReceivePushRequest(std::shared_ptr<TcpClientConnection> conn,
                          const uint8_t *message);
  /// Execute a receive that was in the queue.
  ray::Status ExecuteReceiveObject(const ClientID &client_id, const ObjectID &object_id,
                                   uint64_t object_size,
                                   std::shared_ptr<TcpClientConnection> conn);

  /// Handles receiving a pull request message.
  void ReceivePullRequest(std::shared_ptr<TcpClientConnection> &conn,
                          const uint8_t *message);

  /// Handles connect message of a new client connection.
  void ConnectClient(std::shared_ptr<TcpClientConnection> &conn, const uint8_t *message);
  /// Handles disconnect message of an existing client connection.
  void DisconnectClient(std::shared_ptr<TcpClientConnection> &conn,
                        const uint8_t *message);
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
