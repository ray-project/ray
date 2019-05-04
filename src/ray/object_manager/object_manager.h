#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <random>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"

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
  /// The port that the object manager should use to listen for connections
  /// from other object managers. If this is 0, the object manager will choose
  /// its own port.
  int object_manager_port;
  /// The time in milliseconds to wait before retrying a pull
  /// that fails due to client id lookup.
  uint pull_timeout_ms;
  /// Maximum number of sends allowed.
  int max_sends;
  /// Maximum number of receives allowed.
  int max_receives;
  /// Object chunk size, in bytes
  uint64_t object_chunk_size;
  /// The store socket name.
  std::string store_socket_name;
  /// The time in milliseconds to wait until a Push request
  /// fails due to unsatisfied local object. Special value:
  /// Negative: waiting infinitely.
  /// 0: giving up retrying immediately.
  int push_timeout_ms;
};

struct LocalObjectInfo {
  /// Information from the object store about the object.
  object_manager::protocol::ObjectInfoT object_info;
  /// A map from the ID of a remote object manager to the timestamp of when
  /// the object was last pushed to that object manager (if a push took place).
  std::unordered_map<ClientID, int64_t> recent_pushes;
};

class ObjectManagerInterface {
 public:
  virtual ray::Status Pull(const ObjectID &object_id) = 0;
  virtual void CancelPull(const ObjectID &object_id) = 0;
  virtual ~ObjectManagerInterface(){};
};

// TODO(hme): Add success/failure callbacks for push and pull.
class ObjectManager : public ObjectManagerInterface {
 public:
  /// Takes user-defined ObjectDirectoryInterface implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param main_service The main asio io_service.
  /// \param config ObjectManager configuration.
  /// \param object_directory An object implementing the object directory interface.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         const ObjectManagerConfig &config,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory);

  ~ObjectManager();

  /// Register GCS-related functionality.
  void RegisterGcs();

  /// Subscribe to notifications of objects added to local store.
  /// Upon subscribing, the callback will be invoked for all objects that
  ///
  /// already exist in the local store.
  /// \param callback The callback to invoke when objects are added to the local store.
  /// \return Status of whether adding the subscription succeeded.
  ray::Status SubscribeObjAdded(
      std::function<void(const object_manager::protocol::ObjectInfoT &)> callback);

  /// Subscribe to notifications of objects deleted from local store.
  ///
  /// \param callback The callback to invoke when objects are removed from the local
  /// store.
  /// \return Status of whether adding the subscription succeeded.
  ray::Status SubscribeObjDeleted(std::function<void(const ray::ObjectID &)> callback);

  /// Consider pushing an object to a remote object manager. This object manager
  /// may choose to ignore the Push call (e.g., if Push is called twice in a row
  /// on the same object, the second one might be ignored).
  ///
  /// \param object_id The object's object id.
  /// \param client_id The remote node's client id.
  /// \return Void.
  void Push(const ObjectID &object_id, const ClientID &client_id);

  /// Pull an object from ClientID.
  ///
  /// \param object_id The object's object id.
  /// \return Status of whether the pull request successfully initiated.
  ray::Status Pull(const ObjectID &object_id);

  /// Try to Pull an object from one of its expected client locations. If there
  /// are more client locations to try after this attempt, then this method
  /// will try each of the other clients in succession, with a timeout between
  /// each attempt. If the object is received or if the Pull is Canceled before
  /// the timeout, then no more Pull requests for this object will be sent
  /// to other node managers until TryPull is called again.
  ///
  /// \param object_id The object's object id.
  /// \return Void.
  void TryPull(const ObjectID &object_id);

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

  /// Cancels all requests (Push/Pull) associated with the given ObjectID. This
  /// method is idempotent.
  ///
  /// \param object_id The ObjectID.
  /// \return Void.
  void CancelPull(const ObjectID &object_id);

  /// Callback definition for wait.
  using WaitCallback = std::function<void(const std::vector<ray::ObjectID> &found,
                                          const std::vector<ray::ObjectID> &remaining)>;
  /// Wait until either num_required_objects are located or wait_ms has elapsed,
  /// then invoke the provided callback.
  ///
  /// \param object_ids The object ids to wait on.
  /// \param timeout_ms The time in milliseconds to wait before invoking the callback.
  /// \param num_required_objects The minimum number of objects required before
  /// invoking the callback.
  /// \param wait_local Whether to wait until objects arrive to this node's store.
  /// \param callback Invoked when either timeout_ms is satisfied OR num_ready_objects
  /// is satisfied.
  /// \return Status of whether the wait successfully initiated.
  ray::Status Wait(const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
                   uint64_t num_required_objects, bool wait_local,
                   const WaitCallback &callback);

  /// Free a list of objects from object store.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  ///                   or send it to all the object stores.
  void FreeObjects(const std::vector<ObjectID> &object_ids, bool local_only);

  /// Return profiling information and reset the profiling information.
  ///
  /// \return All profiling information that has accumulated since the last call
  /// to this method.
  ProfileTableDataT GetAndResetProfilingInfo();

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

 private:
  friend class TestObjectManager;

  struct PullRequest {
    PullRequest() : retry_timer(nullptr), timer_set(false), client_locations() {}
    std::unique_ptr<boost::asio::deadline_timer> retry_timer;
    bool timer_set;
    std::vector<ClientID> client_locations;
  };

  struct WaitState {
    WaitState(asio::io_service &service, int64_t timeout_ms, const WaitCallback &callback)
        : timeout_ms(timeout_ms),
          timeout_timer(std::unique_ptr<boost::asio::deadline_timer>(
              new boost::asio::deadline_timer(
                  service, boost::posix_time::milliseconds(timeout_ms)))),
          callback(callback) {}
    /// The period of time to wait before invoking the callback.
    int64_t timeout_ms;
    /// The timer used whenever wait_ms > 0.
    std::unique_ptr<boost::asio::deadline_timer> timeout_timer;
    /// The callback invoked when WaitCallback is complete.
    WaitCallback callback;
    /// Ordered input object_ids.
    std::vector<ObjectID> object_id_order;
    /// The objects that have not yet been found.
    std::unordered_set<ObjectID> remaining;
    /// The objects that have been found.
    std::unordered_set<ObjectID> found;
    /// Objects that have been requested either by Lookup or Subscribe.
    std::unordered_set<ObjectID> requested_objects;
    /// The number of required objects.
    uint64_t num_required_objects;
  };

  /// Creates a wait request and adds it to active_wait_requests_.
  ray::Status AddWaitRequest(const UniqueID &wait_id,
                             const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
                             uint64_t num_required_objects, bool wait_local,
                             const WaitCallback &callback);

  /// Lookup any remaining objects that are not local. This is invoked after
  /// the wait request is created and local objects are identified.
  ray::Status LookupRemainingWaitObjects(const UniqueID &wait_id);

  /// Invoked when lookup for remaining objects has been invoked. This method subscribes
  /// to any remaining objects if wait conditions have not yet been satisfied.
  void SubscribeRemainingWaitObjects(const UniqueID &wait_id);
  /// Completion handler for Wait.
  void WaitComplete(const UniqueID &wait_id);

  /// Spread the Free request to all objects managers.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  void SpreadFreeObjectRequest(const std::vector<ObjectID> &object_ids);

  /// Handle starting, running, and stopping asio io_service.
  void StartIOService();
  void RunSendService();
  void RunReceiveService();
  void StopIOService();

  /// Handle an object being added to this node. This adds the object to the
  /// directory, pushes the object to other nodes if necessary, and cancels any
  /// outstanding Pull requests for the object.
  void HandleObjectAdded(const object_manager::protocol::ObjectInfoT &object_info);

  /// Register object remove with directory.
  void NotifyDirectoryObjectDeleted(const ObjectID &object_id);

  /// Part of an asynchronous sequence of Pull methods.
  /// Uses an existing connection or creates a connection to ClientID.
  /// Executes on main_service_ thread.
  void PullEstablishConnection(const ObjectID &object_id, const ClientID &client_id);

  /// Asynchronously send a pull request via remote object manager connection.
  /// Executes on main_service_ thread.
  ///
  /// \param object_id The ID of the object request.
  /// \param conn The connection to the remote object manager.
  /// \return Void.
  void PullSendRequest(const ObjectID &object_id,
                       std::shared_ptr<SenderConnection> &conn);

  std::shared_ptr<SenderConnection> CreateSenderConnection(
      ConnectionPool::ConnectionType type, RemoteConnectionInfo info);

  /// This is used to notify the main thread that the sending of a chunk has
  /// completed.
  ///
  /// \param object_id The ID of the object that was sent.
  /// \param client_id The ID of the client that the chunk was sent to.
  /// \param chunk_index The index of the chunk.
  /// \param start_time_us The time when the object manager began sending the
  /// chunk.
  /// \param end_time_us The time when the object manager finished sending the
  /// chunk.
  /// \param status The status of the send (e.g., did it succeed or fail).
  /// \return Void.
  void HandleSendFinished(const ObjectID &object_id, const ClientID &client_id,
                          uint64_t chunk_index, double start_time_us, double end_time_us,
                          ray::Status status);

  /// This is used to notify the main thread that the receiving of a chunk has
  /// completed.
  ///
  /// \param object_id The ID of the object that was received.
  /// \param client_id The ID of the client that the chunk was received from.
  /// \param chunk_index The index of the chunk.
  /// \param start_time_us The time when the object manager began receiving the
  /// chunk.
  /// \param end_time_us The time when the object manager finished receiving the
  /// chunk.
  /// \param status The status of the receive (e.g., did it succeed or fail).
  /// \return Void.
  void HandleReceiveFinished(const ObjectID &object_id, const ClientID &client_id,
                             uint64_t chunk_index, double start_time_us,
                             double end_time_us, ray::Status status);

  /// Begin executing a send.
  /// Executes on send_service_ thread pool.
  ray::Status ExecuteSendObject(const UniqueID &push_id, const ClientID &client_id,
                                const ObjectID &object_id, uint64_t data_size,
                                uint64_t metadata_size, uint64_t chunk_index,
                                const RemoteConnectionInfo &connection_info);

  /// This method synchronously sends the object id and object size
  /// to the remote object manager.
  /// Executes on send_service_ thread pool.
  ray::Status SendObjectHeaders(const UniqueID &push_id, const ObjectID &object_id,
                                uint64_t data_size, uint64_t metadata_size,
                                uint64_t chunk_index,
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
  ray::Status ExecuteReceiveObject(const ClientID &client_id, const ObjectID &object_id,
                                   uint64_t data_size, uint64_t metadata_size,
                                   uint64_t chunk_index, TcpClientConnection &conn);

  /// Handles receiving a pull request message.
  void ReceivePullRequest(std::shared_ptr<TcpClientConnection> &conn,
                          const uint8_t *message);
  /// Handles freeing objects request.
  void ReceiveFreeRequest(std::shared_ptr<TcpClientConnection> &conn,
                          const uint8_t *message);

  /// Handles connect message of a new client connection.
  void ConnectClient(std::shared_ptr<TcpClientConnection> &conn, const uint8_t *message);
  /// Handles disconnect message of an existing client connection.
  void DisconnectClient(std::shared_ptr<TcpClientConnection> &conn,
                        const uint8_t *message);
  /// Handle Push task timeout.
  void HandlePushTaskTimeout(const ObjectID &object_id, const ClientID &client_id);

  ClientID client_id_;
  const ObjectManagerConfig config_;
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
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

  /// Mapping from locally available objects to information about those objects
  /// including when the object was last pushed to other object managers.
  std::unordered_map<ObjectID, LocalObjectInfo> local_objects_;

  /// This is used as the callback identifier in Pull for
  /// SubscribeObjectLocations. We only need one identifier because we never need to
  /// subscribe multiple times to the same object during Pull.
  UniqueID object_directory_pull_callback_id_ = UniqueID::from_random();

  /// A set of active wait requests.
  std::unordered_map<UniqueID, WaitState> active_wait_requests_;

  /// Maintains a map of push requests that have not been fulfilled due to an object not
  /// being local. Objects are removed from this map after push_timeout_ms have elapsed.
  std::unordered_map<
      ObjectID,
      std::unordered_map<ClientID, std::unique_ptr<boost::asio::deadline_timer>>>
      unfulfilled_push_requests_;

  /// The objects that this object manager is currently trying to fetch from
  /// remote object managers.
  std::unordered_map<ObjectID, PullRequest> pull_requests_;

  /// Profiling events that are to be batched together and added to the profile
  /// table in the GCS.
  std::vector<ProfileEventT> profile_events_;

  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_H
