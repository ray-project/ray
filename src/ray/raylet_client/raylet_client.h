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

#include <mutex>
#include <unordered_map>
#include <vector>

#include "ray/common/bundle_spec.h"
#include "ray/common/client_connection.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

using ray::ActorCheckpointID;
using ray::ActorID;
using ray::JobID;
using ray::NodeID;
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

/// Interface for pinning objects. Abstract for testing.
class PinObjectsInterface {
 public:
  /// Request to a raylet to pin a plasma object. The callback will be sent via gRPC.
  virtual void PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) = 0;

  virtual ~PinObjectsInterface(){};
};

/// Interface for leasing workers. Abstract for testing.
class WorkerLeaseInterface {
 public:
  /// Requests a worker from the raylet. The callback will be sent via gRPC.
  /// \param resource_spec Resources that should be allocated for the worker.
  /// \param backlog_size The queue length for the given shape on the CoreWorker.
  /// \return ray::Status
  virtual void RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size = -1) = 0;

  /// Returns a worker to the raylet.
  /// \param worker_port The local port of the worker on the raylet node.
  /// \param worker_id The unique worker id of the worker on the raylet node.
  /// \param disconnect_worker Whether the raylet should disconnect the worker.
  /// \return ray::Status
  virtual ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                                   bool disconnect_worker) = 0;

  /// Notify raylets to release unused workers.
  /// \param workers_in_use Workers currently in use.
  /// \param callback Callback that will be called after raylet completes the release of
  /// unused workers. \return ray::Status
  virtual void ReleaseUnusedWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) = 0;

  virtual void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) = 0;

  virtual ~WorkerLeaseInterface(){};
};

/// Interface for leasing resource.
class ResourceReserveInterface {
 public:
  /// Request a raylet to prepare resources of a given bundle for atomic placement group
  /// creation. This is used for the first phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param resource_spec Resources that should be
  /// allocated for the worker.
  /// \return ray::Status
  virtual void PrepareBundleResources(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply>
          &callback) = 0;

  /// Request a raylet to commit resources of a given bundle for atomic placement group
  /// creation. This is used for the first phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param resource_spec Resources that should be
  /// allocated for the worker.
  /// \return ray::Status
  virtual void CommitBundleResources(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) = 0;

  virtual void CancelResourceReserve(
      BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback) = 0;

  virtual void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) = 0;

  virtual ~ResourceReserveInterface(){};
};

/// Interface for waiting dependencies. Abstract for testing.
class DependencyWaiterInterface {
 public:
  /// Wait for the given objects, asynchronously. The core worker is notified when
  /// the wait completes.
  ///
  /// \param references The objects to wait for.
  /// \param tag Value that will be sent to the core worker via gRPC on completion.
  /// \return ray::Status.
  virtual ray::Status WaitForDirectActorCallArgs(
      const std::vector<rpc::ObjectReference> &references, int64_t tag) = 0;

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

  ray::Status WriteMessage(MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);

  ray::Status AtomicRequestReply(MessageType request_type, MessageType reply_type,
                                 std::vector<uint8_t> *reply_message,
                                 flatbuffers::FlatBufferBuilder *fbb = nullptr);

 private:
  /// The connection to raylet.
  std::shared_ptr<ServerConnection> conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the raylet client.
  std::mutex write_mutex_;
};

class RayletClient : public PinObjectsInterface,
                     public WorkerLeaseInterface,
                     public DependencyWaiterInterface,
                     public ResourceReserveInterface {
 public:
  /// Connect to the raylet.
  ///
  /// \param grpc_client gRPC client to the raylet.
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param worker_type The type of the worker. If it is a certain worker type, an
  /// additional message will be sent to register as one.
  /// \param job_id The job ID of the driver or worker.
  /// \param language Language of the worker.
  /// \param ip_address The IP address of the worker.
  /// \param status This will be populated with the result of connection attempt.
  /// \param raylet_id This will be populated with the local raylet's NodeID.
  /// \param system_config This will be populated with internal config parameters
  /// provided by the raylet.
  /// \param port The port that the worker should listen on for gRPC requests. If
  /// 0, the worker should choose a random port.
  RayletClient(boost::asio::io_service &io_service,
               std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
               const std::string &raylet_socket, const WorkerID &worker_id,
               rpc::WorkerType worker_type, const JobID &job_id, const Language &language,
               const std::string &ip_address, Status *status, NodeID *raylet_id,
               int *port, std::unordered_map<std::string, std::string> *system_config,
               const std::string &job_config);

  /// Connect to the raylet via grpc only.
  ///
  /// \param grpc_client gRPC client to the raylet.
  RayletClient(std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client);

  /// Notify the raylet that this client is disconnecting gracefully. This
  /// is used by actors to exit gracefully so that the raylet doesn't
  /// propagate an error message to the driver.
  ///
  /// \return ray::Status.
  ray::Status Disconnect();

  /// Tell the raylet which port this worker's gRPC server is listening on.
  ///
  /// \param The port.
  /// \return ray::Status.
  Status AnnounceWorkerPort(int port);

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
  /// \param object_ids The IDs of the objects to fetch.
  /// \param owner_addresses The addresses of the workers that own the objects.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param mark_worker_blocked Set to false if current task is a direct call task.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                 const std::vector<rpc::Address> &owner_addresses,
                                 bool fetch_only, bool mark_worker_blocked,
                                 const TaskID &current_task_id);

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
  /// \param owner_addresses The addresses of the workers that own the objects.
  /// \param num_returns The number of objects to wait for.
  /// \param timeout_milliseconds Duration, in milliseconds, to wait before returning.
  /// \param wait_local Whether to wait for objects to appear on this node.
  /// \param mark_worker_blocked Set to false if current task is a direct call task.
  /// \param current_task_id The task that called wait.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return ray::Status.
  ray::Status Wait(const std::vector<ObjectID> &object_ids,
                   const std::vector<rpc::Address> &owner_addresses, int num_returns,
                   int64_t timeout_milliseconds, bool wait_local,
                   bool mark_worker_blocked, const TaskID &current_task_id,
                   WaitResultPair *result);

  /// Wait for the given objects, asynchronously. The core worker is notified when
  /// the wait completes.
  ///
  /// \param references The objects to wait for.
  /// \param tag Value that will be sent to the core worker via gRPC on completion.
  /// \return ray::Status.
  ray::Status WaitForDirectActorCallArgs(
      const std::vector<rpc::ObjectReference> &references, int64_t tag) override;

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
  /// \param node_id NodeID where the resource is to be set
  /// \return ray::Status
  ray::Status SetResource(const std::string &resource_name, const double capacity,
                          const ray::NodeID &node_id);

  /// Ask the raylet to spill an object to external storage.
  /// \param object_id The ID of the object to be spilled.
  /// \param callback Callback that will be called after raylet completes the
  /// object spilling (or it fails).
  void RequestObjectSpillage(
      const ObjectID &object_id,
      const rpc::ClientCallback<rpc::RequestObjectSpillageReply> &callback);

  /// Implements WorkerLeaseInterface.
  void RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size) override;

  /// Implements WorkerLeaseInterface.
  ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                           bool disconnect_worker) override;

  /// Implements WorkerLeaseInterface.
  void ReleaseUnusedWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) override;

  void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override;

  /// Implements PrepareBundleResourcesInterface.
  void PrepareBundleResources(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
      override;

  /// Implements CommitBundleResourcesInterface.
  void CommitBundleResources(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
      override;

  /// Implements CancelResourceReserveInterface.
  void CancelResourceReserve(
      BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
      override;

  /// Implements ReleaseUnusedBundlesInterface.
  void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) override;

  void PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override;

  void GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback);

  // Subscribe to receive notification on plasma object
  void SubscribeToPlasma(const ObjectID &object_id, const rpc::Address &owner_address);

  WorkerID GetWorkerID() const { return worker_id_; }

  JobID GetJobID() const { return job_id_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  /// gRPC client to the raylet. Right now, this is only used for a couple
  /// request types.
  std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client_;
  const WorkerID worker_id_;
  const JobID job_id_;
  const std::string job_config_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;
  /// The connection to the raylet server.
  std::unique_ptr<RayletConnection> conn_;
};

}  // namespace raylet

}  // namespace ray
