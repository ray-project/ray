// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/notification/object_store_notification_manager_ipc.h"
#include "ray/object_manager/object_buffer_pool.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/ownership_based_object_directory.h"
#include "ray/object_manager/plasma/store_runner.h"
#include "ray/object_manager/pull_manager.h"
#include "ray/object_manager/push_manager.h"
#include "ray/rpc/object_manager/object_manager_client.h"
#include "ray/rpc/object_manager/object_manager_server.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

struct ObjectManagerConfig {
  /// The port that the object manager should use to listen for connections
  /// from other object managers. If this is 0, the object manager will choose
  /// its own port.
  int object_manager_port;
  /// The object manager's global timer frequency.
  unsigned int timer_freq_ms;
  /// The time in milliseconds to wait before retrying a pull
  /// that fails due to node id lookup.
  unsigned int pull_timeout_ms;
  /// Object chunk size, in bytes
  uint64_t object_chunk_size;
  /// Max object push bytes in flight.
  uint64_t max_bytes_in_flight;
  /// The store socket name.
  std::string store_socket_name;
  /// The time in milliseconds to wait until a Push request
  /// fails due to unsatisfied local object. Special value:
  /// Negative: waiting infinitely.
  /// 0: giving up retrying immediately.
  int push_timeout_ms;
  /// Number of threads of rpc service
  /// Send and receive request in these threads
  int rpc_service_threads_number;
  /// Initial memory allocation for store.
  int64_t object_store_memory = -1;
  /// The directory for shared memory files.
  std::string plasma_directory;
  /// Enable huge pages.
  bool huge_pages;
};

struct LocalObjectInfo {
  /// Information from the object store about the object.
  object_manager::protocol::ObjectInfoT object_info;
};
class ObjectStoreRunner {
 public:
  ObjectStoreRunner(const ObjectManagerConfig &config,
                    SpillObjectsCallback spill_objects_callback,
                    std::function<void()> object_store_full_callback);
  ~ObjectStoreRunner();

 private:
  std::thread store_thread_;
};

class ObjectManagerInterface {
 public:
  virtual uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs) = 0;
  virtual void CancelPull(uint64_t request_id) = 0;
  virtual ~ObjectManagerInterface(){};
};

// TODO(hme): Add success/failure callbacks for push and pull.
class ObjectManager : public ObjectManagerInterface,
                      public rpc::ObjectManagerServiceHandler {
 public:
  using RestoreSpilledObjectCallback = std::function<void(
      const ObjectID &, const std::string &, std::function<void(const ray::Status &)>)>;

  /// Implementation of object manager service

  /// Handle push request from remote object manager
  ///
  /// Push request will contain the object which is specified by pull request
  /// the object will be transfered by a sequence of chunks.
  ///
  /// \param request Push request including the object chunk data
  /// \param reply Reply to the sender
  /// \param send_reply_callback Callback of the request
  void HandlePush(const rpc::PushRequest &request, rpc::PushReply *reply,
                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle pull request from remote object manager
  ///
  /// \param request Pull request
  /// \param reply Reply
  /// \param send_reply_callback Callback of request
  void HandlePull(const rpc::PullRequest &request, rpc::PullReply *reply,
                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle free objects request
  ///
  /// \param request Free objects request
  /// \param reply Reply
  /// \param send_reply_callback
  void HandleFreeObjects(const rpc::FreeObjectsRequest &request,
                         rpc::FreeObjectsReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  /// Send object to remote object manager
  ///
  /// Object will be transfered as a sequence of chunks, small object(defined in config)
  /// contains only one chunk
  /// \param push_id Unique push id to indicate this push request
  /// \param object_id Object id
  /// \param owner_address The address of the object's owner
  /// \param node_id The id of the receiver.
  /// \param data_size Data size
  /// \param metadata_size Metadata size
  /// \param chunk_index Chunk index of this object chunk, start with 0
  /// \param rpc_client Rpc client used to send message to remote object manager
  /// \param on_complete Callback to run on completion.
  void SendObjectChunk(const UniqueID &push_id, const ObjectID &object_id,
                       const rpc::Address &owner_address, const NodeID &node_id,
                       uint64_t data_size, uint64_t metadata_size, uint64_t chunk_index,
                       std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                       std::function<void(const Status &)> on_complete);

  /// Receive object chunk from remote object manager, small object may contain one chunk
  ///
  /// \param node_id Node id of remote object manager which sends this chunk
  /// \param object_id Object id
  /// \param owner_address The address of the object's owner
  /// \param data_size Data size
  /// \param metadata_size Metadata size
  /// \param chunk_index Chunk index
  /// \param data Chunk data
  ray::Status ReceiveObjectChunk(const NodeID &node_id, const ObjectID &object_id,
                                 const rpc::Address &owner_address, uint64_t data_size,
                                 uint64_t metadata_size, uint64_t chunk_index,
                                 const std::string &data);

  /// Send pull request
  ///
  /// \param object_id Object id
  /// \param client_id Remote server client id
  void SendPullRequest(const ObjectID &object_id, const NodeID &client_id);

  /// Get the rpc client according to the node ID
  ///
  /// \param node_id Remote node id, will send rpc request to it
  std::shared_ptr<rpc::ObjectManagerClient> GetRpcClient(const NodeID &node_id);

  /// Get the port of the object manager rpc server.
  int GetServerPort() const { return object_manager_server_.GetPort(); }

 public:
  /// Takes user-defined ObjectDirectoryInterface implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param main_service The main asio io_service.
  /// \param config ObjectManager configuration.
  /// \param object_directory An object implementing the object directory interface.
  explicit ObjectManager(boost::asio::io_service &main_service,
                         const NodeID &self_node_id, const ObjectManagerConfig &config,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory,
                         RestoreSpilledObjectCallback restore_spilled_object,
                         SpillObjectsCallback spill_objects_callback = nullptr,
                         std::function<void()> object_store_full_callback = nullptr);

  ~ObjectManager();

  /// Stop the Plasma Store eventloop. Currently it is only used to handle
  /// signals from Raylet.
  void Stop();

  /// This methods call the plasma store which runs in a separate thread.
  /// Check if the given object id is evictable by directly calling plasma store.
  /// Plasma store will return true if the object is spillable, meaning it is only
  /// pinned by the raylet, so we can comfotable evict after spilling the object from
  /// local object manager. False otherwise.
  bool IsPlasmaObjectSpillable(const ObjectID &object_id);

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
  /// \param node_id The remote node's id.
  /// \return Void.
  void Push(const ObjectID &object_id, const NodeID &node_id);

  /// Pull a bundle of objects. This will attempt to make all objects in the
  /// bundle local until the request is canceled with the returned ID.
  ///
  /// \param object_refs The bundle of objects that must be made local.
  /// \return A request ID that can be used to cancel the request.
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs) override;

  /// Cancels the pull request with the given ID. This cancels any fetches for
  /// objects that were passed to the original pull request, if no other pull
  /// request requires them.
  ///
  /// \param pull_request_id The request to cancel.
  void CancelPull(uint64_t pull_request_id) override;

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
  /// \param callback Invoked when either timeout_ms is satisfied OR num_ready_objects
  /// is satisfied.
  /// \return Status of whether the wait successfully initiated.
  ray::Status Wait(const std::vector<ObjectID> &object_ids,
                   const std::unordered_map<ObjectID, rpc::Address> &owner_addresses,
                   int64_t timeout_ms, uint64_t num_required_objects,
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
  std::shared_ptr<rpc::ProfileTableData> GetAndResetProfilingInfo();

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

  /// Populate object store stats.
  ///
  /// \param Output parameter.
  void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const;

  void Tick(const boost::system::error_code &e);

 private:
  friend class TestObjectManager;

  struct WaitState {
    WaitState(boost::asio::io_service &service, int64_t timeout_ms,
              const WaitCallback &callback)
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
    /// Objects' owners.
    std::unordered_map<ObjectID, rpc::Address> owner_addresses;
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
  ray::Status AddWaitRequest(
      const UniqueID &wait_id, const std::vector<ObjectID> &object_ids,
      const std::unordered_map<ObjectID, rpc::Address> &owner_addresses,
      int64_t timeout_ms, uint64_t num_required_objects, const WaitCallback &callback);

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
  void SpreadFreeObjectsRequest(
      const std::vector<ObjectID> &object_ids,
      const std::vector<std::shared_ptr<rpc::ObjectManagerClient>> &rpc_clients);

  /// Handle starting, running, and stopping asio rpc_service.
  void StartRpcService();
  void RunRpcService(int index);
  void StopRpcService();

  /// Handle an object being added to this node. This adds the object to the
  /// directory, pushes the object to other nodes if necessary, and cancels any
  /// outstanding Pull requests for the object.
  void HandleObjectAdded(const object_manager::protocol::ObjectInfoT &object_info);

  /// Register object remove with directory.
  void NotifyDirectoryObjectDeleted(const ObjectID &object_id);

  /// This is used to notify the main thread that the sending of a chunk has
  /// completed.
  ///
  /// \param object_id The ID of the object that was sent.
  /// \param node_id The ID of the node that the chunk was sent to.
  /// \param chunk_index The index of the chunk.
  /// \param start_time_us The time when the object manager began sending the
  /// chunk.
  /// \param end_time_us The time when the object manager finished sending the
  /// chunk.
  /// \param status The status of the send (e.g., did it succeed or fail).
  /// \return Void.
  void HandleSendFinished(const ObjectID &object_id, const NodeID &node_id,
                          uint64_t chunk_index, double start_time_us, double end_time_us,
                          ray::Status status);

  /// This is used to notify the main thread that the receiving of a chunk has
  /// completed.
  ///
  /// \param object_id The ID of the object that was received.
  /// \param node_id The ID of the node that the chunk was received from.
  /// \param chunk_index The index of the chunk.
  /// \param start_time_us The time when the object manager began receiving the
  /// chunk.
  /// \param end_time_us The time when the object manager finished receiving the
  /// chunk.
  /// \param status The status of the receive (e.g., did it succeed or fail).
  /// \return Void.
  void HandleReceiveFinished(const ObjectID &object_id, const NodeID &node_id,
                             uint64_t chunk_index, double start_time_us,
                             double end_time_us, ray::Status status);

  /// Handle Push task timeout.
  void HandlePushTaskTimeout(const ObjectID &object_id, const NodeID &node_id);

  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  boost::asio::io_service *main_service_;

  NodeID self_node_id_;
  const ObjectManagerConfig config_;
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  // Object store runner.
  ObjectStoreRunner object_store_internal_;
  // Process notifications from Plasma. We make it a shared pointer because
  // we will decide its type at runtime, and we would pass it to Plasma Store.
  std::shared_ptr<ObjectStoreNotificationManager> store_notification_;
  ObjectBufferPool buffer_pool_;

  /// Multi-thread asio service, deal with all outgoing and incoming RPC request.
  boost::asio::io_service rpc_service_;

  /// Keep rpc service running when no task in rpc service.
  boost::asio::io_service::work rpc_work_;

  /// The thread pool used for running `rpc_service`.
  /// Data copy operations during request are done in this thread pool.
  std::vector<std::thread> rpc_threads_;

  /// Mapping from locally available objects to information about those objects
  /// including when the object was last pushed to other object managers.
  std::unordered_map<ObjectID, LocalObjectInfo> local_objects_;

  /// This is used as the callback identifier in Pull for
  /// SubscribeObjectLocations. We only need one identifier because we never need to
  /// subscribe multiple times to the same object during Pull.
  UniqueID object_directory_pull_callback_id_ = UniqueID::FromRandom();

  /// A set of active wait requests.
  std::unordered_map<UniqueID, WaitState> active_wait_requests_;

  /// Maintains a map of push requests that have not been fulfilled due to an object not
  /// being local. Objects are removed from this map after push_timeout_ms have elapsed.
  std::unordered_map<
      ObjectID, std::unordered_map<NodeID, std::unique_ptr<boost::asio::deadline_timer>>>
      unfulfilled_push_requests_;

  /// Profiling events that are to be batched together and added to the profile
  /// table in the GCS.
  std::vector<rpc::ProfileTableData::ProfileEvent> profile_events_;

  /// mutex lock used to protect profile_events_, profile_events_ is used in main thread
  /// and rpc thread.
  std::mutex profile_mutex_;

  /// The gPRC server.
  rpc::GrpcServer object_manager_server_;

  /// The gRPC service.
  rpc::ObjectManagerGrpcService object_manager_service_;

  /// The client call manager used to deal with reply.
  rpc::ClientCallManager client_call_manager_;

  /// Client id - object manager gRPC client.
  std::unordered_map<NodeID, std::shared_ptr<rpc::ObjectManagerClient>>
      remote_object_manager_clients_;

  const RestoreSpilledObjectCallback restore_spilled_object_;

  /// Pull manager retry timer .
  boost::asio::deadline_timer pull_retry_timer_;

  /// Object push manager.
  std::unique_ptr<PushManager> push_manager_;

  /// Object pull manager.
  std::unique_ptr<PullManager> pull_manager_;

  /// Running sum of the amount of memory used in the object store.
  int64_t used_memory_ = 0;

  /// Running total of received chunks.
  int64_t num_chunks_received_total_ = 0;

  /// Running total of received chunks that failed (duplicated).
  int64_t num_chunks_received_failed_ = 0;
};

}  // namespace ray
