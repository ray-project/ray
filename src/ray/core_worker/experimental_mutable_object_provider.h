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
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/core_worker/experimental_mutable_object_provider_interface.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace core {
namespace experimental {

// This class coordinates the transfer of mutable objects between different nodes. It
// handles mutable objects that are received from remote nodes, and it also observes local
// mutable objects and pushes them to remote nodes as needed.
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
  absl::flat_hash_map<ObjectID, LocalReaderInfo> remote_writer_object_to_local_reader_
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
  absl::flat_hash_map<ObjectID, uint64_t> written_so_far_
      ABSL_GUARDED_BY(written_so_far_lock_);

  friend class MutableObjectProvider_MutableObjectBufferReadRelease_Test;
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
