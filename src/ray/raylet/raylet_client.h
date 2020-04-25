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

#ifndef RAYLET_CLIENT_H
#define RAYLET_CLIENT_H

#include <ray/protobuf/gcs.pb.h>
#include <unistd.h>

#include <boost/asio/detail/socket_holder.hpp>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/node_manager/node_manager_client.h"

using ray::ActorCheckpointID;
using ray::ActorID;
using ray::ClientID;
using ray::JobID;
using ray::ObjectID;
using ray::TaskID;
using ray::WorkerID;

using ray::Language;
using ray::rpc::ProfileTableData;

using MessageType = ray::protocol::MessageType;
using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;
using WaitResultPair = std::pair<std::vector<ObjectID>, std::vector<ObjectID>>;

namespace ray {

typedef boost::asio::generic::stream_protocol local_stream_protocol;
typedef boost::asio::basic_stream_socket<local_stream_protocol> local_stream_socket;

/// Interface for pinning objects. Abstract for testing.
class PinObjectsInterface {
 public:
  /// Request to a raylet to pin a plasma object. The callback will be sent via gRPC.
  virtual ray::Status PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) = 0;

  virtual ~PinObjectsInterface(){};
};

/// Interface for leasing workers. Abstract for testing.
class WorkerLeaseInterface {
 public:
  /// Requests a worker from the raylet. The callback will be sent via gRPC.
  /// \param resource_spec Resources that should be allocated for the worker.
  /// \return ray::Status
  virtual ray::Status RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback) = 0;

  /// Returns a worker to the raylet.
  /// \param worker_port The local port of the worker on the raylet node.
  /// \param worker_id The unique worker id of the worker on the raylet node.
  /// \param disconnect_worker Whether the raylet should disconnect the worker.
  /// \return ray::Status
  virtual ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                                   bool disconnect_worker) = 0;

  virtual ray::Status CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) = 0;

  virtual ~WorkerLeaseInterface(){};
};

/// Interface for waiting dependencies. Abstract for testing.
class DependencyWaiterInterface {
 public:
  /// Wait for the given objects, asynchronously. The core worker is notified when
  /// the wait completes.
  ///
  /// \param object_ids The objects to wait for.
  /// \param tag Value that will be sent to the core worker via gRPC on completion.
  /// \return ray::Status.
  virtual ray::Status WaitForDirectActorCallArgs(const std::vector<ObjectID> &object_ids,
                                                 int64_t tag) = 0;

  virtual ~DependencyWaiterInterface(){};
};

namespace raylet {

class RayletConnection {
 public:
  /// Connect to the raylet.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  ///        additional message will be sent to register as one.
  /// \param job_id The ID of the driver. This is non-nil if the client is a
  ///        driver.
  /// \return The connection information.
  RayletConnection(boost::asio::io_service &io_service, const std::string &raylet_socket,
                   int num_retries, int64_t timeout);

  /// Notify the raylet that this client is disconnecting gracefully. This
  /// is used by actors to exit gracefully so that the raylet doesn't
  /// propagate an error message to the driver.
  ///
  /// \return ray::Status.
  ray::Status Disconnect();

  ray::Status ReadMessage(MessageType type, std::unique_ptr<uint8_t[]> &message);

  ray::Status WriteMessage(MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);

  ray::Status AtomicRequestReply(MessageType request_type, MessageType reply_type,
                                 std::unique_ptr<uint8_t[]> &reply_message,
                                 flatbuffers::FlatBufferBuilder *fbb = nullptr);

 private:
  /// The Unix domain socket that connects to raylet.
  local_stream_socket conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the raylet client.
  std::mutex write_mutex_;
};

class RayletClient : public PinObjectsInterface,
                     public WorkerLeaseInterface,
                     public DependencyWaiterInterface {
 public:
  /// Connect to the raylet.
  ///
  /// \param grpc_client gRPC client to the raylet.
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  /// additional message will be sent to register as one.
  /// \param job_id The ID of the driver. This is non-nil if the client is a driver.
  /// \param language Language of the worker.
  /// \param raylet_id This will be populated with the local raylet's ClientID.
  /// \param ip_address The IP address of the worker.
  /// \param port The port that the worker will listen on for gRPC requests, if
  /// any.
  RayletClient(boost::asio::io_service &io_service,
               std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
               const std::string &raylet_socket, const WorkerID &worker_id,
               bool is_worker, const JobID &job_id, const Language &language,
               ClientID *raylet_id, const std::string &ip_address, int port = -1);

  /// Connect to the raylet via grpc only.
  ///
  /// \param grpc_client gRPC client to the raylet.
  RayletClient(std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client);

  ray::Status Disconnect() { return conn_->Disconnect(); };

  /// Submit a task using the raylet code path.
  ///
  /// \param The task specification.
  /// \return ray::Status.
  ray::Status SubmitTask(const ray::TaskSpecification &task_spec);

  /// Tell the raylet that the client has finished executing a task.
  ///
  /// \return ray::Status.
  ray::Status TaskDone();

  /// Tell the raylet to reconstruct or fetch objects.
  ///
  /// \param object_ids The IDs of the objects to reconstruct.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param mark_worker_blocked Set to false if current task is a direct call task.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids, bool fetch_only,
                                 bool mark_worker_blocked, const TaskID &current_task_id);

  /// Notify the raylet that this client (worker) is no longer blocked.
  ///
  /// \param current_task_id The task that is no longer blocked.
  /// \return ray::Status.
  ray::Status NotifyUnblocked(const TaskID &current_task_id);

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

  /// Wait for the given objects until timeout expires or num_return objects are
  /// found.
  ///
  /// \param object_ids The objects to wait for.
  /// \param num_returns The number of objects to wait for.
  /// \param timeout_milliseconds Duration, in milliseconds, to wait before returning.
  /// \param wait_local Whether to wait for objects to appear on this node.
  /// \param mark_worker_blocked Set to false if current task is a direct call task.
  /// \param current_task_id The task that called wait.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return ray::Status.
  ray::Status Wait(const std::vector<ObjectID> &object_ids, int num_returns,
                   int64_t timeout_milliseconds, bool wait_local,
                   bool mark_worker_blocked, const TaskID &current_task_id,
                   WaitResultPair *result);

  /// Wait for the given objects, asynchronously. The core worker is notified when
  /// the wait completes.
  ///
  /// \param object_ids The objects to wait for.
  /// \param tag Value that will be sent to the core worker via gRPC on completion.
  /// \return ray::Status.
  ray::Status WaitForDirectActorCallArgs(const std::vector<ObjectID> &object_ids,
                                         int64_t tag) override;

  /// Push an error to the relevant driver.
  ///
  /// \param The ID of the job_id that the error is for.
  /// \param The type of the error.
  /// \param The error message.
  /// \param The timestamp of the error.
  /// \return ray::Status.
  ray::Status PushError(const ray::JobID &job_id, const std::string &type,
                        const std::string &error_message, double timestamp);

  /// Store some profile events in the GCS.
  ///
  /// \param profile_events A batch of profiling event information.
  /// \return ray::Status.
  ray::Status PushProfileEvents(const ProfileTableData &profile_events);

  /// Free a list of objects from object stores.
  ///
  /// \param object_ids A list of ObjectsIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  /// or send it to all the object stores.
  /// \param delete_creating_tasks Whether also delete objects' creating tasks from GCS.
  /// \return ray::Status.
  ray::Status FreeObjects(const std::vector<ray::ObjectID> &object_ids, bool local_only,
                          bool deleteCreatingTasks);

  /// Request raylet backend to prepare a checkpoint for an actor.
  ///
  /// \param[in] actor_id ID of the actor.
  /// \param[out] checkpoint_id ID of the new checkpoint (output parameter).
  /// \return ray::Status.
  ray::Status PrepareActorCheckpoint(const ActorID &actor_id,
                                     ActorCheckpointID *checkpoint_id);

  /// Notify raylet backend that an actor was resumed from a checkpoint.
  ///
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the checkpoint from which the actor was resumed.
  /// \return ray::Status.
  ray::Status NotifyActorResumedFromCheckpoint(const ActorID &actor_id,
                                               const ActorCheckpointID &checkpoint_id);

  /// Sets a resource with the specified capacity and client id
  /// \param resource_name Name of the resource to be set
  /// \param capacity Capacity of the resource
  /// \param client_Id ClientID where the resource is to be set
  /// \return ray::Status
  ray::Status SetResource(const std::string &resource_name, const double capacity,
                          const ray::ClientID &client_Id);

  /// Implements WorkerLeaseInterface.
  ray::Status RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback)
      override;

  /// Implements WorkerLeaseInterface.
  ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                           bool disconnect_worker) override;

  ray::Status CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override;

  ray::Status PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override;

  ray::Status GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback);

  // Subscribe to receive notification on plasma object
  ray::Status SubscribeToPlasma(const ObjectID &object_id);

  WorkerID GetWorkerID() const { return worker_id_; }

  JobID GetJobID() const { return job_id_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  /// gRPC client to the raylet. Right now, this is only used for a couple
  /// request types.
  std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client_;
  const WorkerID worker_id_;
  const JobID job_id_;
  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;
  /// The connection to the raylet server.
  std::unique_ptr<RayletConnection> conn_;
};

}  // namespace raylet

}  // namespace ray

#endif
