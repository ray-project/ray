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

#include "ray/common/client_connection.h"
#include "ray/id.h"
#include "ray/status.h"

#include "ray/object_manager/connection_pool.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_buffer_pool.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/object_manager_client_connection.h"
#include "ray/object_manager/object_store_notification_manager.h"

namespace ray {

struct ObjectManagerConfig {
  /// The time in milliseconds to wait before retrying a pull
  /// that failed due to client id lookup.
  uint pull_timeout_ms;
  /// Maximum number of sends allowed.
  int max_sends;
  /// Maximum number of receives allowed.
  int max_receives;
  /// Object chunk size, in bytes
  uint64_t object_chunk_size;
  /// The stored socked name.
  std::string store_socket_name;
  /// The time in milliseconds to wait until a Push request
  /// failed due to unsatisfied local object.
  int push_timeout_ms;
};

class ObjectManagerInterface {
 public:
  virtual ray::Status Pull(const ObjectID &object_id) = 0;
  virtual ray::Status Cancel(const ObjectID &object_id) = 0;
  virtual ~ObjectManagerInterface(){};
};

// TODO(hme): Add success/failure callbacks for push and pull.
class ObjectManager : public ObjectManagerInterface {
 public:
  /// Implicitly instantiates Ray implementation of ObjectDirectory.
  ///
  /// \param main_service The main asio io_service.
  /// \param config ObjectManager configuration.
  /// \param gcs_client A client connection to the Ray GCS.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         const ObjectManagerConfig &config,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client);

  /// Takes user-defined ObjectDirectoryInterface implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param main_service The main asio io_service.
  /// \param config ObjectManager configuration.
  /// \param od An object implementing the object directory interface.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         const ObjectManagerConfig &config,
                         std::unique_ptr<ObjectDirectoryInterface> od);

  ~ObjectManager();

  /// Register GCS-related functionality.
  void RegisterGcs();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  ///
  /// already exist in the local store.
  /// \param callback The callback to invoke when objects are added to the local store.
  /// \return Status of whether adding the subscription succeeded.
  ray::Status SubscribeObjAdded(std::function<void(const ObjectInfoT &)> callback);

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
  void ProcessNewClient(TcpClientConnection &conn);

  /// Process messages sent from other nodes. We only establish
  /// transfer connections using this method; all other transfer communication
  /// is done separately.
  ///
  /// \param conn The connection.
  /// \param message_type The message type.
  /// \param message A pointer set to the beginning of the message.
  void ProcessClientMessage(std::shared_ptr<TcpClientConnection> &conn,
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

 private:
  ClientID client_id_;
  const ObjectManagerConfig config_;
  std::unique_ptr<ObjectDirectoryInterface> object_directory_;
  ObjectStoreNotificationManager store_notification_;
  ObjectBufferPool buffer_pool_;

  /// This runs on a thread pool dedicated to sending objects.
  boost::asio::io_service send_service_;
  /// This runs on a thread pool dedicated to receiving objects.
  boost::asio::io_service receive_service_;

  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  boost::asio::io_service *main_service_;

  /// Used to create "work" for send_service_.
  /// Without this, if send_service_ has no more sends to process, it will stop.
  boost::asio::io_service::work send_work_;
  /// Used to create "work" for receive_service_.
  /// Without this, if receive_service_ has no more receives to process, it will stop.
  boost::asio::io_service::work receive_work_;

  /// Runs the send service, which handle
  /// all outgoing object transfers.
  std::vector<std::thread> send_threads_;
  /// Runs the receive service, which handle
  /// all incoming object transfers.
  std::vector<std::thread> receive_threads_;

  /// Connection pool for reusing outgoing connections to remote object managers.
  ConnectionPool connection_pool_;

  /// Cache of locally available objects.
  std::unordered_map<ObjectID, ObjectInfoT> local_objects_;

  /// Unfulfilled Push tasks.
  /// The timer is for removing a push task due to unsatisfied local object.
  std::unordered_map<
      ObjectID,
      std::unordered_map<ClientID, std::shared_ptr<boost::asio::deadline_timer>>>
      unfulfilled_push_tasks_;

  /// Handle starting, running, and stopping asio io_service.
  void StartIOService();
  void RunSendService();
  void RunReceiveService();
  void StopIOService();

  /// Register object add with directory.
  void NotifyDirectoryObjectAdd(const ObjectInfoT &object_info);

  /// Register object remove with directory.
  void NotifyDirectoryObjectDeleted(const ObjectID &object_id);

  /// Part of an asynchronous sequence of Pull methods.
  /// Uses an existing connection or creates a connection to ClientID.
  /// Executes on main_service_ thread.
  ray::Status PullEstablishConnection(const ObjectID &object_id,
                                      const ClientID &client_id);

  /// Private callback implementation for success on get location. Called from
  /// ObjectDirectory.
  void GetLocationsSuccess(const std::vector<ray::ClientID> &client_ids,
                           const ray::ObjectID &object_id);

  /// Synchronously send a pull request via remote object manager connection.
  /// Executes on main_service_ thread.
  ray::Status PullSendRequest(const ObjectID &object_id,
                              std::shared_ptr<SenderConnection> &conn);

  std::shared_ptr<SenderConnection> CreateSenderConnection(
      ConnectionPool::ConnectionType type, RemoteConnectionInfo info);

  /// Begin executing a send.
  /// Executes on send_service_ thread pool.
  void ExecuteSendObject(const ClientID &client_id, const ObjectID &object_id,
                         uint64_t data_size, uint64_t metadata_size, uint64_t chunk_index,
                         const RemoteConnectionInfo &connection_info);
  /// This method synchronously sends the object id and object size
  /// to the remote object manager.
  /// Executes on send_service_ thread pool.
  ray::Status SendObjectHeaders(const ObjectID &object_id, uint64_t data_size,
                                uint64_t metadata_size, uint64_t chunk_index,
                                std::shared_ptr<SenderConnection> &conn);

  /// This method initiates the actual object transfer.
  /// Executes on send_service_ thread pool.
  ray::Status SendObjectData(const ObjectID &object_id,
                             const ObjectBufferPool::ChunkInfo &chunk_info,
                             std::shared_ptr<SenderConnection> &conn);

  /// Invoked when a remote object manager pushes an object to this object manager.
  /// This will invoke the object receive on the receive_service_ thread pool.
  void ReceivePushRequest(std::shared_ptr<TcpClientConnection> &conn,
                          const uint8_t *message);
  /// Execute a receive on the receive_service_ thread pool.
  void ExecuteReceiveObject(const ClientID &client_id, const ObjectID &object_id,
                            uint64_t data_size, uint64_t metadata_size,
                            uint64_t chunk_index, TcpClientConnection &conn);

  /// Handles receiving a pull request message.
  void ReceivePullRequest(std::shared_ptr<TcpClientConnection> &conn,
                          const uint8_t *message);

  /// Handles connect message of a new client connection.
  void ConnectClient(std::shared_ptr<TcpClientConnection> &conn, const uint8_t *message);
  /// Handles disconnect message of an existing client connection.
  void DisconnectClient(std::shared_ptr<TcpClientConnection> &conn,
                        const uint8_t *message);
  /// Handle Push task timeout.
  void HandlePushTaskTimeout(const ObjectID &object_id, const ClientID &client_id);
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
