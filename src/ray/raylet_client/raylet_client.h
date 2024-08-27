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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/client_connection.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

using ray::ActorID;
using ray::JobID;
using ray::NodeID;
using ray::ObjectID;
using ray::TaskID;
using ray::WorkerID;

using ray::Language;

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
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) = 0;

  virtual ~PinObjectsInterface(){};
};

/// Interface for leasing workers. Abstract for testing.
class WorkerLeaseInterface {
 public:
  /// Requests a worker from the raylet. The callback will be sent via gRPC.
  /// \param resource_spec Resources that should be allocated for the worker.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param callback: The callback to call when the request finishes.
  /// \param backlog_size The queue length for the given shape on the CoreWorker.
  virtual void RequestWorkerLease(
      const rpc::TaskSpec &task_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size = -1,
      const bool is_selected_based_on_locality = false) = 0;

  /// Returns a worker to the raylet.
  /// \param worker_port The local port of the worker on the raylet node.
  /// \param worker_id The unique worker id of the worker on the raylet node.
  /// \param disconnect_worker Whether the raylet should disconnect the worker.
  /// \param worker_exiting Whether the worker is exiting and cannot be reused.
  /// \return ray::Status
  virtual ray::Status ReturnWorker(int worker_port,
                                   const WorkerID &worker_id,
                                   bool disconnect_worker,
                                   const std::string &disconnect_worker_error_detail,
                                   bool worker_exiting) = 0;

  /// Notify raylets to release unused workers.
  /// \param workers_in_use Workers currently in use.
  /// \param callback Callback that will be called after raylet completes the release of
  /// unused workers. \return ray::Status
  virtual void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) = 0;

  virtual void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) = 0;

  /// Report the backlog size of a given worker and a given scheduling class to the
  /// raylet.
  /// \param worker_id The ID of the worker that reports the backlog size.
  /// \param backlog_reports The backlog report for each scheduling class
  virtual void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) = 0;

  virtual void GetTaskFailureCause(
      const TaskID &task_id,
      const ray::rpc::ClientCallback<ray::rpc::GetTaskFailureCauseReply> &callback) = 0;

  virtual ~WorkerLeaseInterface(){};
};

/// Interface for leasing resource.
class ResourceReserveInterface {
 public:
  /// Request a raylet to prepare resources of given bundles for atomic placement group
  /// creation. This is used for the first phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param bundle_specs Bundles to be scheduled at this raylet.
  /// \return ray::Status
  virtual void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply>
          &callback) = 0;

  /// Request a raylet to commit resources of given bundles for atomic placement group
  /// creation. This is used for the second phase of atomic placement group creation. The
  /// callback will be sent via gRPC.
  /// \param bundle_specs Bundles to be scheduled at this raylet.
  /// \return ray::Status
  virtual void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback) = 0;

  virtual void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
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

/// Inteface for getting resource reports.
class ResourceTrackingInterface {
 public:
  virtual void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) = 0;

  virtual ~ResourceTrackingInterface(){};
};

class MutableObjectReaderInterface {
 public:
  /// Registers a mutable object on this node so that it can be read. Writes are performed
  /// on a remote node. This local node creates a mapping from `object_id` ->
  /// `reader_ref`.
  ///
  /// \param writer_object_id The object ID of the mutable object on the remote node that
  /// is written to.
  /// \param num_readers The number of readers that will read the object on this local
  /// node.
  /// \param reader_object_id The object ID of the mutable object that is read on this
  /// local node.
  /// \param callback This callback is executed to send a reply to the remote
  /// node once the mutable object is registered.
  virtual void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const rpc::ClientCallback<rpc::RegisterMutableObjectReply> &callback) = 0;

  /// Handles a mutable object write that was performed on a remote node and is being
  /// transferred to this node so that it can be read.
  ///
  /// \param writer_object_id The object ID of the mutable object on the remote node that
  /// is written to. This is *not* the object ID of the corresponding mutable object on
  /// this local node.
  /// \param data_size The size of the data to write to the mutable object on this local
  /// node.
  /// \param metadata_size The size of the metadata to write to the mutable object on this
  /// local node.
  /// \param data The data and metadata to write. This is formatted as (data | metadata).
  /// \param callback This callback is executed to send a reply to the remote node once
  /// the mutable object is transferred.
  virtual void PushMutableObject(
      const ObjectID &writer_object_id,
      uint64_t data_size,
      uint64_t metadata_size,
      void *data,
      const rpc::ClientCallback<rpc::PushMutableObjectReply> &callback) = 0;
};

class RayletClientInterface : public PinObjectsInterface,
                              public WorkerLeaseInterface,
                              public DependencyWaiterInterface,
                              public ResourceReserveInterface,
                              public ResourceTrackingInterface,
                              public MutableObjectReaderInterface {
 public:
  virtual ~RayletClientInterface(){};

  /// Get the system config from Raylet.
  /// \param callback Callback that will be called after raylet replied the system config.
  virtual void GetSystemConfig(
      const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) = 0;

  virtual void NotifyGCSRestart(
      const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) = 0;

  virtual void ShutdownRaylet(
      const NodeID &node_id,
      bool graceful,
      const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) = 0;

  virtual void DrainRaylet(
      const rpc::autoscaler::DrainNodeReason &reason,
      const std::string &reason_message,
      int64_t deadline_timestamp_ms,
      const rpc::ClientCallback<rpc::DrainRayletReply> &callback) = 0;

  virtual std::shared_ptr<grpc::Channel> GetChannel() const = 0;
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
  RayletConnection(instrumented_io_context &io_service,
                   const std::string &raylet_socket,
                   int num_retries,
                   int64_t timeout);

  ray::Status WriteMessage(MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);

  ray::Status AtomicRequestReply(MessageType request_type,
                                 MessageType reply_type,
                                 std::vector<uint8_t> *reply_message,
                                 flatbuffers::FlatBufferBuilder *fbb = nullptr);

 private:
  /// Shutdown the raylet if the local connection is disconnected.
  void ShutdownIfLocalRayletDisconnected(const Status &status);
  /// The connection to raylet.
  std::shared_ptr<ServerConnection> conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the raylet client.
  std::mutex write_mutex_;
};

class RayletClient : public RayletClientInterface {
 public:
  /// Connect to the raylet.
  ///
  /// \param grpc_client gRPC client to the raylet.
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param worker_type The type of the worker. If it is a certain worker type, an
  /// additional message will be sent to register as one.
  /// \param job_id The job ID of the driver or worker.
  /// \param runtime_env_hash The hash of the runtime env of the worker.
  /// \param language Language of the worker.
  /// \param ip_address The IP address of the worker.
  /// \param status This will be populated with the result of connection attempt.
  /// \param raylet_id This will be populated with the local raylet's NodeID.
  /// \param port The port that the worker should listen on for gRPC requests. If
  /// 0, the worker should choose a random port.
  /// \param system_config This will be populated with internal config parameters
  /// provided by the raylet.
  /// \param serialized_job_config If this is a driver connection, the job config
  /// provided by driver will be passed to Raylet.
  /// \param startup_token The startup token of the process assigned to
  /// it during startup as a command line argument.
  RayletClient(instrumented_io_context &io_service,
               std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client,
               const std::string &raylet_socket,
               const WorkerID &worker_id,
               rpc::WorkerType worker_type,
               const JobID &job_id,
               const int &runtime_env_hash,
               const Language &language,
               const std::string &ip_address,
               Status *status,
               NodeID *raylet_id,
               int *port,
               const std::string &serialized_job_config,
               StartupToken startup_token);

  /// Connect to the raylet via grpc only.
  ///
  /// \param grpc_client gRPC client to the raylet.
  RayletClient(std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client);

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

  /// Tell the raylet to reconstruct or fetch objects.
  ///
  /// \param object_ids The IDs of the objects to fetch.
  /// \param owner_addresses The addresses of the workers that own the objects.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                 const std::vector<rpc::Address> &owner_addresses,
                                 bool fetch_only,
                                 const TaskID &current_task_id);

  /// Notify the raylet that this client (worker) is no longer blocked.
  ///
  /// \param current_task_id The task that is no longer blocked.
  /// \return ray::Status.
  ray::Status NotifyUnblocked(const TaskID &current_task_id);

  /// Notify the raylet that this client is blocked. This is only used for direct task
  /// calls. Note that ordering of this with respect to Unblock calls is important.
  ///
  /// \param release_resources: true if the dirct call blocking needs to release
  /// resources. \return ray::Status.
  ray::Status NotifyDirectCallTaskBlocked(bool release_resources);

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
  /// \param current_task_id The task that called wait.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return ray::Status.
  ray::Status Wait(const std::vector<ObjectID> &object_ids,
                   const std::vector<rpc::Address> &owner_addresses,
                   int num_returns,
                   int64_t timeout_milliseconds,
                   const TaskID &current_task_id,
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

  std::shared_ptr<grpc::Channel> GetChannel() const override;

  /// Implements WorkerLeaseInterface.
  void RequestWorkerLease(
      const rpc::TaskSpec &resource_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size,
      const bool is_selected_based_on_locality) override;

  /// Implements WorkerLeaseInterface.
  ray::Status ReturnWorker(int worker_port,
                           const WorkerID &worker_id,
                           bool disconnect_worker,
                           const std::string &disconnect_worker_error_detail,
                           bool worker_exiting) override;

  void GetTaskFailureCause(
      const TaskID &task_id,
      const ray::rpc::ClientCallback<ray::rpc::GetTaskFailureCauseReply> &callback)
      override;

  /// Implements MutableObjectReaderInterface.
  void RegisterMutableObjectReader(
      const ObjectID &writer_object_id,
      int64_t num_readers,
      const ObjectID &reader_object_id,
      const ray::rpc::ClientCallback<ray::rpc::RegisterMutableObjectReply> &callback)
      override;

  /// Implements MutableObjectReaderInterface.
  void PushMutableObject(const ObjectID &writer_object_id,
                         uint64_t data_size,
                         uint64_t metadata_size,
                         void *data,
                         const ray::rpc::ClientCallback<ray::rpc::PushMutableObjectReply>
                             &callback) override;

  /// Implements WorkerLeaseInterface.
  void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override;

  /// Implements WorkerLeaseInterface.
  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) override;

  void CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override;

  /// Implements PrepareBundleResourcesInterface.
  void PrepareBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback)
      override;

  /// Implements CommitBundleResourcesInterface.
  void CommitBundleResources(
      const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
      const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback)
      override;

  /// Implements CancelResourceReserveInterface.
  void CancelResourceReserve(
      const BundleSpecification &bundle_spec,
      const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback)
      override;

  /// Implements ReleaseUnusedBundlesInterface.
  void ReleaseUnusedBundles(
      const std::vector<rpc::Bundle> &bundles_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback) override;

  void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override;

  void ShutdownRaylet(
      const NodeID &node_id,
      bool graceful,
      const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback) override;

  void DrainRaylet(const rpc::autoscaler::DrainNodeReason &reason,
                   const std::string &reason_message,
                   int64_t deadline_timestamp_ms,
                   const rpc::ClientCallback<rpc::DrainRayletReply> &callback) override;

  void GetSystemConfig(
      const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback) override;

  void GlobalGC(const rpc::ClientCallback<rpc::GlobalGCReply> &callback);

  void GetResourceLoad(
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override;

  void NotifyGCSRestart(
      const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback) override;

  // Subscribe to receive notification on plasma object
  void SubscribeToPlasma(const ObjectID &object_id, const rpc::Address &owner_address);

  WorkerID GetWorkerID() const { return worker_id_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

  int64_t GetPinsInFlight() const { return pins_in_flight_.load(); }

 private:
  /// gRPC client to the raylet. Right now, this is only used for a couple
  /// request types.
  std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client_;
  const WorkerID worker_id_;

  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;
  /// The connection to the raylet server.
  std::unique_ptr<RayletConnection> conn_;

  /// The number of object ID pin RPCs currently in flight.
  std::atomic<int64_t> pins_in_flight_{0};

 protected:
  RayletClient() {}
};

}  // namespace raylet

}  // namespace ray
