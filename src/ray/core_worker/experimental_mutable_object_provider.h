// Copyright 2024 The Ray Authors.
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

#include <memory>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace core {
namespace experimental {

// This class coordinates the transfer of mutable objects between different nodes. It
// handles mutable objects that are received from remote nodes, and it also observes local
// mutable objects and pushes them to remote nodes as needed.
class MutableObjectProviderInterface {
 public:
  virtual ~MutableObjectProviderInterface() = default;

  /// Registers a reader channel for `object_id` on this node.
  /// \param[in] object_id The ID of the object.
  virtual void RegisterReaderChannel(const ObjectID &object_id) = 0;

  /// Registers a writer channel for `object_id` on this node. On each write to this
  /// channel, the write will be sent via RPC to node `node_id`.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] remote_reader_node_ids The list of remote reader's node ids.
  virtual void RegisterWriterChannel(
      const ObjectID &writer_object_id,
      const std::vector<NodeID> &remote_reader_node_ids) = 0;

  /// Handles an RPC request from another note to register a mutable object on this node.
  /// The remote node writes the object and this node reads the object. This node is
  /// notified of writes to the object via HandlePushMutableObject().
  /// \param[in] writer_object_id The ID of the object on the remote note.
  /// \param[in] num_readers The number of readers on this node.
  /// \param[in] reader_object_id The ID of the corresponding object on this node. When
  /// this node is notified of a write via HandlePushMutableObject(), the
  /// `reader_object_id` object is updated with the write.
  virtual void HandleRegisterMutableObject(const ObjectID &writer_object_id,
                                           int64_t num_readers,
                                           const ObjectID &reader_object_id) = 0;

  /// RPC callback for when a writer pushes a mutable object over the network to a reader
  /// on this node.
  virtual void HandlePushMutableObject(const rpc::PushMutableObjectRequest &request,
                                       rpc::PushMutableObjectReply *reply) = 0;

  /// Acquires a write lock on the object that prevents readers from reading
  /// until we are done writing. This is safe for concurrent writers.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] data_size The size of the object to write. This overwrites the
  /// current data size.
  /// \param[in] metadata A pointer to the object metadata buffer to copy. This
  /// will overwrite the current metadata.
  /// \param[in] metadata_size The number of bytes to copy from the metadata
  /// pointer.
  /// \param[in] num_readers The number of readers that must read and release
  /// value we will write before the next WriteAcquire can proceed. The readers
  /// may not start reading until WriteRelease is called.
  /// \param[out] data The mutable object buffer in plasma that can be written to.
  /// \param[in] timeout_ms The timeout in milliseconds to acquire the write lock.
  /// If this is 0, the method will try to acquire the write lock once immediately,
  /// and return either OK or TimedOut without blocking. If this is -1, the method
  /// will block indefinitely until the write lock is acquired.
  /// \return The return status.
  virtual Status WriteAcquire(const ObjectID &object_id,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              int64_t num_readers,
                              std::shared_ptr<Buffer> &data,
                              int64_t timeout_ms = -1) = 0;

  /// Releases an acquired write lock on the object, allowing readers to read.
  /// This is the equivalent of "Seal" for normal objects.
  ///
  /// \param[in] object_id The ID of the object.
  /// \return The return status.
  virtual Status WriteRelease(const ObjectID &object_id) = 0;

  /// Acquires a read lock on the object that prevents the writer from writing
  /// again until we are done reading the current value.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[out] result The read object. This buffer is guaranteed to be valid
  /// until the caller calls ReadRelease next.
  /// \param[in] timeout_ms The timeout in milliseconds to acquire the read lock.
  /// If this is 0, the method will try to acquire the read lock once immediately,
  /// and return either OK or TimedOut without blocking. If this is -1, the method
  /// will block indefinitely until the read lock is acquired.
  /// \return The return status. The ReadAcquire can fail if there have already
  /// been `num_readers` for the current value.
  virtual Status ReadAcquire(const ObjectID &object_id,
                             std::shared_ptr<RayObject> &result,
                             int64_t timeout_ms = -1) = 0;

  /// Releases the object, allowing it to be written again. If the caller did
  /// not previously ReadAcquire the object, then this first blocks until the
  /// latest value is available to read, then releases the value.
  ///
  /// \param[in] object_id The ID of the object.
  virtual Status ReadRelease(const ObjectID &object_id) = 0;

  /// Sets the error bit, causing all future readers and writers to raise an
  /// error on acquire.
  ///
  /// \param[in] object_id The ID of the object.
  virtual Status SetError(const ObjectID &object_id) = 0;

  /// Returns the current status of the channel for the object. Possible statuses are:
  /// 1. Status::OK()
  //     - The channel is registered and open.
  /// 2. Status::ChannelError()
  ///    - The channel was registered and previously open, but is now closed.
  /// 3. Status::NotFound()
  ///    - No channel exists for this object.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] is_reader Whether the channel is a reader channel.
  /// \return Current status of the channel.
  virtual Status GetChannelStatus(const ObjectID &object_id, bool is_reader) = 0;
};

class MutableObjectProvider : public MutableObjectProviderInterface {
 public:
  using RayletFactory =
      std::function<std::shared_ptr<RayletClientInterface>(const NodeID &)>;

  MutableObjectProvider(std::shared_ptr<plasma::PlasmaClientInterface> plasma,
                        RayletFactory raylet_client_factory,
                        std::function<Status(void)> check_signals);

  ~MutableObjectProvider() override;

  void RegisterReaderChannel(const ObjectID &object_id) override;

  void RegisterWriterChannel(const ObjectID &writer_object_id,
                             const std::vector<NodeID> &remote_reader_node_ids) override;

  void HandleRegisterMutableObject(const ObjectID &writer_object_id,
                                   int64_t num_readers,
                                   const ObjectID &reader_object_id) override;

  void HandlePushMutableObject(const rpc::PushMutableObjectRequest &request,
                               rpc::PushMutableObjectReply *reply) override;

  Status WriteAcquire(const ObjectID &object_id,
                      int64_t data_size,
                      const uint8_t *metadata,
                      int64_t metadata_size,
                      int64_t num_readers,
                      std::shared_ptr<Buffer> &data,
                      int64_t timeout_ms = -1) override;

  Status WriteRelease(const ObjectID &object_id) override;

  Status ReadAcquire(const ObjectID &object_id,
                     std::shared_ptr<RayObject> &result,
                     int64_t timeout_ms = -1) override;

  Status ReadRelease(const ObjectID &object_id) override;

  Status SetError(const ObjectID &object_id) override;

  Status GetChannelStatus(const ObjectID &object_id, bool is_reader) override;

 private:
  struct LocalReaderInfo {
    int64_t num_readers{};
    ObjectID local_object_id;
  };

  /// Listens for local changes to `object_id` and sends the changes to remote nodes via
  /// the network.
  ///
  /// \param[in] io_context The IO context.
  /// \param[in] writer_object_id The object ID of the writer.
  /// \param[in] remote_readers A list of remote reader clients.
  void PollWriterClosure(
      instrumented_io_context &io_context,
      const ObjectID &writer_object_id,
      const std::shared_ptr<std::vector<std::shared_ptr<RayletClientInterface>>>
          &remote_readers);

  // Kicks off `io_context`.
  void RunIOContext(instrumented_io_context &io_context);

  // The plasma store.
  std::shared_ptr<plasma::PlasmaClientInterface> plasma_;

  // Object manager for the mutable objects.
  std::shared_ptr<ray::experimental::MutableObjectManager> object_manager_;

  // Protects `remote_writer_object_to_local_reader_`.
  absl::Mutex remote_writer_object_to_local_reader_lock_;
  // Maps the remote node object ID (i.e., the object ID that the remote node writes to)
  // to the corresponding local object ID (i.e., the object ID that the local node reads
  // from) and the number of readers.
  std::unordered_map<ObjectID, LocalReaderInfo> remote_writer_object_to_local_reader_
      ABSL_GUARDED_BY(remote_writer_object_to_local_reader_lock_);

  // Creates a Raylet client for each mutable object. When the polling thread detects a
  // write to the mutable object, this client sends the updated mutable object via RPC to
  // the Raylet on the remote node.
  RayletFactory raylet_client_factory_;

  // Each mutable object that requires inter-node communication has its own thread and
  // event loop. Thus, all of the objects below are vectors, with each vector index
  // corresponding to a different mutable object.
  // Keeps alive the event loops for RPCs for inter-node communication of mutable objects.
  std::vector<std::unique_ptr<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>>
      io_works_;
  // Contexts in which the application looks for local changes to mutable objects and
  // sends the changes to remote nodes via the network.
  std::vector<std::unique_ptr<instrumented_io_context>> io_contexts_;
  // Manage outgoing RPCs that send mutable object changes to remote nodes.
  std::vector<std::unique_ptr<rpc::ClientCallManager>> client_call_managers_;
  // Threads that wait for local mutable object changes (one thread per mutable object)
  // and then send the changes to remote nodes via the network.
  std::vector<std::unique_ptr<std::thread>> io_threads_;

  // Protects the `written_so_far_` map.
  absl::Mutex written_so_far_lock_;
  // For objects larger than the gRPC max payload size *that this node receives from a
  // writer node*, this map tracks how many bytes have been received so far for a single
  // object write.
  std::unordered_map<ObjectID, uint64_t> written_so_far_
      ABSL_GUARDED_BY(written_so_far_lock_);

  friend class MutableObjectProvider_MutableObjectBufferReadRelease_Test;
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
