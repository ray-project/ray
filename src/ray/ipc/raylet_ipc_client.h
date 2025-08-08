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
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/flatbuffers/node_manager_generated.h"
#include "ray/ipc/client_connection.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/common.pb.h"

using MessageType = ray::protocol::MessageType;

namespace ray {

namespace ipc {

/// Client for interacting with the local Raylet over a socket.
///
/// Message ordering on the socket is guaranteed.
///
/// If the socket is broken and the local Raylet is detected to be dead, calling any
/// method on the client will quick exit the process.
class RayletIpcClient {
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

  /// Register this client (worker) with the local Raylet.
  ///
  /// \param worker_id The worker_id of the connecting worker.
  /// \param worker_type The worker type of the connecting worker.
  /// \param job_id The job ID that the connecting worker is associated with.
  /// \param runtime_env_hash The runtime_env hash of the connecting worker.
  /// \param language The language of the connecting worker.
  /// \param ip_address The ip_address of the connecting worker.
  /// \param serialized_job_config The serialized job config of the connecting worker.
  /// \param startup_token The token that was passed to this worker at startup.
  /// \param[out] raylet_id The node ID for the local Raylet.
  /// \param[out] assigned_port The assigned port for the worker to listen on. If zero,
  ///             the worker should pick a port randomly.
  ray::Status RegisterClient(const WorkerID &worker_id,
                             rpc::WorkerType worker_type,
                             const JobID &job_id,
                             int runtime_env_hash,
                             const rpc::Language &language,
                             const std::string &ip_address,
                             const std::string &serialized_job_config,
                             const StartupToken &startup_token,
                             NodeID *raylet_id,
                             int *assigned_port);

  /// Notify the raylet that this client is disconnecting gracefully. This
  /// is used by actors to exit gracefully so that the raylet doesn't
  /// propagate an error message to the driver.
  ///
  /// It's a blocking call.
  ///
  /// \param disconnect_type The reason why this worker process is disconnected.
  /// \param disconnect_detail The detailed reason for a given exit.
  /// \return ray::Status.
  ray::Status Disconnect(
      const rpc::WorkerExitType &exit_type,
      const std::string &exit_detail,
      const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes);

  /// Tell the raylet which port this worker's gRPC server is listening on.
  ///
  /// \param port The port.
  /// \return ray::Status.
  Status AnnounceWorkerPortForWorker(int port);

  /// Tell the raylet this driver and its job is ready to run, with port and entrypoint.
  ///
  /// \param port The port.
  /// \param entrypoint The entrypoint of the driver's job.
  /// \return ray::Status.
  Status AnnounceWorkerPortForDriver(int port, const std::string &entrypoint);

  /// Tell the raylet that the client has finished executing a task.
  ///
  /// \return ray::Status.
  ray::Status ActorCreationTaskDone();

  /// Ask the Raylet to pull a set of objects to the local node.
  ///
  /// This request is asynchronous.
  ///
  /// \param object_ids The IDs of the objects to pull.
  /// \param owner_addresses The owner addresses of the objects.
  /// \return ray::Status.
  ray::Status AsyncGetObjects(const std::vector<ObjectID> &object_ids,
                              const std::vector<rpc::Address> &owner_addresses);

  /// Wait for the given objects until timeout expires or num_return objects are
  /// found.
  ///
  /// \param object_ids The objects to wait for.
  /// \param owner_addresses The addresses of the workers that own the objects.
  /// \param num_returns The number of objects to wait for.
  /// \param timeout_milliseconds Duration, in milliseconds, to wait before returning.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return ray::StatusOr containing error status or the set of object ids that were
  /// found.
  ray::StatusOr<absl::flat_hash_set<ObjectID>> Wait(
      const std::vector<ObjectID> &object_ids,
      const std::vector<rpc::Address> &owner_addresses,
      int num_returns,
      int64_t timeout_milliseconds);

  /// Tell the Raylet to cancel the get request from this worker.
  ///
  /// \return ray::Status.
  ray::Status CancelGetRequest();

  /// Notify the raylet that this client is blocked. This is only used for direct task
  /// calls. Note that ordering of this with respect to Unblock calls is important.
  ///
  /// \return ray::Status.
  ray::Status NotifyDirectCallTaskBlocked();

  /// Notify the raylet that this client is unblocked. This is only used for direct task
  /// calls. Note that ordering of this with respect to Block calls is important.
  ///
  /// \return ray::Status.
  ray::Status NotifyDirectCallTaskUnblocked();

  /// Wait for the given objects asynchronously.
  ///
  /// The core worker will be notified over gRPC when the wait completes.
  ///
  /// \param references The objects to wait for.
  /// \param tag Value that will be sent to the core worker via gRPC on completion.
  /// \return ray::Status.
  ray::Status WaitForActorCallArgs(const std::vector<rpc::ObjectReference> &references,
                                   int64_t tag);

  /// Push an error to the relevant driver.
  ///
  /// \param The ID of the job_id that the error is for.
  /// \param The type of the error.
  /// \param The error message.
  /// \param The timestamp of the error.
  /// \return ray::Status.
  ray::Status PushError(const ray::JobID &job_id,
                        const std::string &type,
                        const std::string &error_message,
                        double timestamp);

  /// Free a list of objects from object stores.
  ///
  /// \param object_ids A list of ObjectsIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  /// or send it to all the object stores.
  /// \return ray::Status.
  ray::Status FreeObjects(const std::vector<ray::ObjectID> &object_ids, bool local_only);

  /// Subscribe this worker to a notification when the provided object is ready in the
  /// local object store.
  ///
  /// The worker will be notified over gRPC when the object is ready.
  ///
  /// \param object_id The ID of the object to subscribe to.
  /// \param owner_address The address of the owner of the object.
  void SubscribePlasmaReady(const ObjectID &object_id, const rpc::Address &owner_address);

 private:
  /// Send a request to raylet asynchronously.
  ray::Status WriteMessage(MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);

  /// Send a request to raylet and synchronously wait for the response.
  ray::Status AtomicRequestReply(MessageType request_type,
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
