// Copyright 2025 The Ray Authors.
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
#include <mutex>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/flatbuffers/node_manager_generated.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/common.pb.h"

using MessageType = ray::protocol::MessageType;

namespace ray {
namespace ipc {

class RayletIpcClient : public RayletIpcClientInterface {
 public:
  /// Connect to the Raylet over a local socket.
  ///
  /// \param io_service The IO service used for interacting with the socket.
  /// \param address The address of the socket that the Raylet is listening on.
  /// \param num_retries The number of times to retry connecting before giving up.
  /// \param timeout The time to wait between retries.
  RayletIpcClient(instrumented_io_context &io_service,
                  const std::string &address,
                  int num_retries,
                  int64_t timeout);

  Status RegisterClient(const WorkerID &worker_id,
                        rpc::WorkerType worker_type,
                        const JobID &job_id,
                        int runtime_env_hash,
                        const rpc::Language &language,
                        const std::string &ip_address,
                        const std::string &serialized_job_config,
                        const StartupToken &startup_token,
                        NodeID *node_id,
                        int *assigned_port) override;

  Status Disconnect(const rpc::WorkerExitType &exit_type,
                    const std::string &exit_detail,
                    const std::shared_ptr<LocalMemoryBuffer>
                        &creation_task_exception_pb_bytes) override;

  Status AnnounceWorkerPortForWorker(int port) override;

  Status AnnounceWorkerPortForDriver(int port, const std::string &entrypoint) override;

  Status ActorCreationTaskDone() override;

  Status AsyncGetObjects(const std::vector<ObjectID> &object_ids,
                         const std::vector<rpc::Address> &owner_addresses) override;

  StatusOr<absl::flat_hash_set<ObjectID>> Wait(
      const std::vector<ObjectID> &object_ids,
      const std::vector<rpc::Address> &owner_addresses,
      int num_returns,
      int64_t timeout_milliseconds) override;

  Status CancelGetRequest() override;

  /// Notify the raylet that the worker is currently blocked waiting for an object
  /// to be pulled. The raylet will release the resources used by this worker.
  ///
  /// \return Status::OK if no error occurs.
  /// \return Status::IOError if any error occurs.
  Status NotifyWorkerBlocked() override;

  /// Notify the raylet that the worker is unblocked. The raylet will cancel inflight
  /// pull requests for the worker.
  ///
  /// \return Status::OK if no error occurs.
  /// \return Status::IOError if any error occurs.
  Status NotifyWorkerUnblocked() override;

  Status WaitForActorCallArgs(const std::vector<rpc::ObjectReference> &references,
                              int64_t tag) override;

  Status PushError(const JobID &job_id,
                   const std::string &type,
                   const std::string &error_message,
                   double timestamp) override;

  Status FreeObjects(const std::vector<ObjectID> &object_ids, bool local_only) override;

  void SubscribePlasmaReady(const ObjectID &object_id,
                            const rpc::Address &owner_address) override;

 private:
  /// Send a request to raylet asynchronously.
  Status WriteMessage(MessageType type, flatbuffers::FlatBufferBuilder *fbb = nullptr);

  /// Send a request to raylet and synchronously wait for the response.
  Status AtomicRequestReply(MessageType request_type,
                            MessageType reply_type,
                            std::vector<uint8_t> *reply_message,
                            flatbuffers::FlatBufferBuilder *fbb = nullptr);

  /// Protects read operations on the socket.
  std::mutex mutex_;

  /// Protects write operations on the socket.
  std::mutex write_mutex_;

  /// The local socket connection to the Raylet.
  std::shared_ptr<ServerConnection> conn_;
};

}  // namespace ipc
}  // namespace ray
