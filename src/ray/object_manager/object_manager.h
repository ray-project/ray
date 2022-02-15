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
#include <boost/bind/bind.hpp>
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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/chunk_object_reader.h"
#include "ray/object_manager/common.h"
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
  /// The IP address this object manager is running on.
  std::string object_manager_address;
  /// The port that the object manager should use to listen for connections
  /// from other object managers. If this is 0, the object manager will choose
  /// its own port.
  int object_manager_port;
  /// The object manager's global timer frequency.
  unsigned int timer_freq_ms;
  /// The time in milliseconds to wait before retrying a pull
  /// that failed.
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
  /// The directory for fallback allocation files.
  std::string fallback_directory;
  /// Enable huge pages.
  bool huge_pages;
};

struct LocalObjectInfo {
  /// Information from the object store about the object.
  ObjectInfo object_info;
};
class ObjectStoreRunner {
 public:
  ObjectStoreRunner(const ObjectManagerConfig &config,
                    SpillObjectsCallback spill_objects_callback,
                    std::function<void()> object_store_full_callback,
                    AddObjectCallback add_object_callback,
                    DeleteObjectCallback delete_object_callback);
  ~ObjectStoreRunner();

 private:
  std::thread store_thread_;
};

class ObjectManagerInterface {
 public:
  virtual uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                        BundlePriority prio) = 0;
  virtual void CancelPull(uint64_t request_id) = 0;
  virtual bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const = 0;
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

  /// Get the port of the object manager rpc server.
  int GetServerPort() const { return object_manager_server_.GetPort(); }

  bool PullRequestActiveOrWaitingForMetadata(uint64_t pull_request_id) const override {
    return pull_manager_->PullRequestActiveOrWaitingForMetadata(pull_request_id);
  }

 public:
  /// Takes user-defined IObjectDirectory implementation.
  /// When this constructor is used, the ObjectManager assumes ownership of
  /// the given ObjectDirectory instance.
  ///
  /// \param main_service The main asio io_service.
  /// \param config ObjectManager configuration.
  /// \param object_directory An object implementing the object directory interface.
  explicit ObjectManager(
      instrumented_io_context &main_service, const NodeID &self_node_id,
      const ObjectManagerConfig &config, IObjectDirectory *object_directory,
      RestoreSpilledObjectCallback restore_spilled_object,
      std::function<std::string(const ObjectID &)> get_spilled_object_url,
      SpillObjectsCallback spill_objects_callback,
      std::function<void()> object_store_full_callback,
      AddObjectCallback add_object_callback, DeleteObjectCallback delete_object_callback,
      std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object,
      const std::function<void(const ObjectID &)> fail_pull_request);

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
  /// \param prio The bundle priority.
  /// \return A request ID that can be used to cancel the request.
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                BundlePriority prio) override;

  /// Cancels the pull request with the given ID. This cancels any fetches for
  /// objects that were passed to the original pull request, if no other pull
  /// request requires them.
  ///
  /// \param pull_request_id The request to cancel.
  void CancelPull(uint64_t pull_request_id) override;

  /// Free a list of objects from object store.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  ///                   or send it to all the object stores.
  void FreeObjects(const std::vector<ObjectID> &object_ids, bool local_only);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record the internal stats.
  void RecordMetrics();

  /// Populate object store stats.
  ///
  /// \param Output parameter.
  void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const;

  void Tick(const boost::system::error_code &e);

  /// Get the current object store memory usage.
  int64_t GetUsedMemory() const { return used_memory_; }

  int64_t GetMemoryCapacity() const { return config_.object_store_memory; }

  double GetUsedMemoryPercentage() const {
    return static_cast<double>(used_memory_) / config_.object_store_memory;
  }

  bool PullManagerHasPullsQueued() const { return pull_manager_->HasPullsQueued(); }

 private:
  friend class TestObjectManager;

  /// Spread the Free request to all objects managers.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  void SpreadFreeObjectsRequest(
      const std::vector<ObjectID> &object_ids,
      const std::vector<std::shared_ptr<rpc::ObjectManagerClient>> &rpc_clients);

  /// Pushing a known local object to a remote object manager.
  ///
  /// \param object_id The object's object id.
  /// \param node_id The remote node's id.
  /// \return Void.
  void PushLocalObject(const ObjectID &object_id, const NodeID &node_id);

  /// Pushing a known spilled object to a remote object manager.
  /// \param object_id The object's object id.
  /// \param node_id The remote node's id.
  /// \param spilled_url The url of the spilled object.
  /// \return Void.
  void PushFromFilesystem(const ObjectID &object_id, const NodeID &node_id,
                          const std::string &spilled_url);

  /// The internal implementation of pushing an object.
  ///
  /// \param object_id The object's id.
  /// \param node_id The remote node's id.
  /// \param chunk_reader Chunk reader used to read a chunk of the object
  /// Status::OK() if the read succeeded.
  void PushObjectInternal(const ObjectID &object_id, const NodeID &node_id,
                          std::shared_ptr<ChunkObjectReader> chunk_reader);

  /// Send one chunk of the object to remote object manager
  ///
  /// Object will be transfered as a sequence of chunks, small object(defined in config)
  /// contains only one chunk
  /// \param push_id Unique push id to indicate this push request
  /// \param object_id Object id
  /// \param node_id The id of the receiver.
  /// \param chunk_index Chunk index of this object chunk, start with 0
  /// \param rpc_client Rpc client used to send message to remote object manager
  /// \param on_complete Callback when the chunk is sent
  /// \param chunk_reader Chunk reader used to read a chunk of the object
  void SendObjectChunk(const UniqueID &push_id, const ObjectID &object_id,
                       const NodeID &node_id, uint64_t chunk_index,
                       std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                       std::function<void(const Status &)> on_complete,
                       std::shared_ptr<ChunkObjectReader> chunk_reader);

  /// Handle starting, running, and stopping asio rpc_service.
  void StartRpcService();
  void RunRpcService(int index);
  void StopRpcService();

  /// Handle an object being added to this node. This adds the object to the
  /// directory, pushes the object to other nodes if necessary, and cancels any
  /// outstanding Pull requests for the object.
  void HandleObjectAdded(const ObjectInfo &object_info);

  /// Handle an object being deleted from this node. This registers object remove
  /// with directory. This also asks the pull manager to fetch this object again
  /// as soon as possible.
  void HandleObjectDeleted(const ObjectID &object_id);

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

  /// Handle Push task timeout.
  void HandlePushTaskTimeout(const ObjectID &object_id, const NodeID &node_id);

  /// Receive an object chunk from a remote object manager. Small object may
  /// fit in one chunk.
  ///
  /// If this is the last remaining chunk for an object, then the object will
  /// be sealed. Else, we will keep the plasma buffer open until the remaining
  /// chunks are received.
  ///
  /// If the object is no longer being actively pulled, the object will not be
  /// created.
  ///
  /// \param node_id Node id of remote object manager which sends this chunk
  /// \param object_id Object id
  /// \param owner_address The address of the object's owner
  /// \param data_size Data size
  /// \param metadata_size Metadata size
  /// \param chunk_index Chunk index
  /// \param data Chunk data
  /// \return Whether the chunk was successfully written into the local object
  /// store. This can fail if the chunk was already received in the past, or if
  /// the object is no longer being actively pulled.
  bool ReceiveObjectChunk(const NodeID &node_id, const ObjectID &object_id,
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

  /// Weak reference to main service. We ensure this object is destroyed before
  /// main_service_ is stopped.
  instrumented_io_context *main_service_;

  NodeID self_node_id_;
  const ObjectManagerConfig config_;
  /// The object directory interface to access object information.
  IObjectDirectory *object_directory_;

  /// Object store runner.
  ObjectStoreRunner object_store_internal_;

  ObjectBufferPool buffer_pool_;

  /// Multi-thread asio service, deal with all outgoing and incoming RPC request.
  instrumented_io_context rpc_service_;

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

  /// Maintains a map of push requests that have not been fulfilled due to an object not
  /// being local. Objects are removed from this map after push_timeout_ms have elapsed.
  std::unordered_map<
      ObjectID, std::unordered_map<NodeID, std::unique_ptr<boost::asio::deadline_timer>>>
      unfulfilled_push_requests_;

  /// The gPRC server.
  rpc::GrpcServer object_manager_server_;

  /// The gRPC service.
  rpc::ObjectManagerGrpcService object_manager_service_;

  /// The client call manager used to deal with reply.
  rpc::ClientCallManager client_call_manager_;

  /// Client id - object manager gRPC client.
  std::unordered_map<NodeID, std::shared_ptr<rpc::ObjectManagerClient>>
      remote_object_manager_clients_;

  /// Callback to trigger direct restoration of an object.
  const RestoreSpilledObjectCallback restore_spilled_object_;

  /// Callback to get the URL of a locally spilled object.
  /// This returns the empty string if the object was not spilled locally.
  std::function<std::string(const ObjectID &)> get_spilled_object_url_;

  /// Pull manager retry timer .
  boost::asio::deadline_timer pull_retry_timer_;

  /// Object push manager.
  std::unique_ptr<PushManager> push_manager_;

  /// Object pull manager.
  std::unique_ptr<PullManager> pull_manager_;

  /// Running sum of the amount of memory used in the object store.
  int64_t used_memory_ = 0;

  /// Running total of received chunks.
  size_t num_chunks_received_total_ = 0;

  /// Running total of received chunks that failed. A finer-grained breakdown
  /// is recorded below.
  size_t num_chunks_received_total_failed_ = 0;

  /// The total number of chunks that we failed to receive because they were
  /// no longer needed by any worker or task on this node.
  size_t num_chunks_received_cancelled_ = 0;

  /// The total number of chunks that we failed to receive because we could not
  /// create the object in plasma. This is usually due to out-of-memory in
  /// plasma.
  size_t num_chunks_received_failed_due_to_plasma_ = 0;
};

}  // namespace ray
