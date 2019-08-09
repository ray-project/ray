#ifndef RAYLET_CLIENT_H
#define RAYLET_CLIENT_H

#include <ray/protobuf/gcs.pb.h>
#include <unistd.h>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"

using ray::ActorCheckpointID;
using ray::ActorID;
using ray::ClientID;
using ray::JobID;
using ray::ObjectID;
using ray::TaskID;
using ray::UniqueID;

using ray::Language;
using ray::rpc::ProfileTableData;

using MessageType = ray::protocol::MessageType;
using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;
using WaitResultPair = std::pair<std::vector<ObjectID>, std::vector<ObjectID>>;

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
  RayletConnection(const std::string &raylet_socket, int num_retries, int64_t timeout);

  ~RayletConnection() { close(conn_); }
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
  /// File descriptor of the Unix domain socket that connects to raylet.
  int conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the raylet client.
  std::mutex write_mutex_;
};

class RayletClient {
 public:
  /// Connect to the raylet.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  /// additional message will be sent to register as one.
  /// \param job_id The ID of the driver. This is non-nil if the client is a driver.
  /// \return The connection information.
  RayletClient(const std::string &raylet_socket, const ClientID &client_id,
               bool is_worker, const JobID &job_id, const Language &language,
               int port = -1);

  ray::Status Disconnect() { return conn_->Disconnect(); };

  /// Submit a task using the raylet code path.
  ///
  /// \param The task specification.
  /// \return ray::Status.
  ray::Status SubmitTask(const ray::TaskSpecification &task_spec);

  /// Get next task for this client. This will block until the scheduler assigns
  /// a task to this worker. The caller takes ownership of the returned task
  /// specification and must free it.
  ///
  /// \param task_spec The assigned task.
  /// \return ray::Status.
  ray::Status GetTask(std::unique_ptr<ray::TaskSpecification> *task_spec);

  /// Tell the raylet that the client has finished executing a task.
  ///
  /// \return ray::Status.
  ray::Status TaskDone();

  /// Tell the raylet to reconstruct or fetch objects.
  ///
  /// \param object_ids The IDs of the objects to reconstruct.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids, bool fetch_only,
                                 const TaskID &current_task_id);
  /// Notify the raylet that this client (worker) is no longer blocked.
  ///
  /// \param current_task_id The task that is no longer blocked.
  /// \return ray::Status.
  ray::Status NotifyUnblocked(const TaskID &current_task_id);

  /// Wait for the given objects until timeout expires or num_return objects are
  /// found.
  ///
  /// \param object_ids The objects to wait for.
  /// \param num_returns The number of objects to wait for.
  /// \param timeout_milliseconds Duration, in milliseconds, to wait before returning.
  /// \param wait_local Whether to wait for objects to appear on this node.
  /// \param current_task_id The task that called wait.
  /// \param result A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  /// \return ray::Status.
  ray::Status Wait(const std::vector<ObjectID> &object_ids, int num_returns,
                   int64_t timeout_milliseconds, bool wait_local,
                   const TaskID &current_task_id, WaitResultPair *result);

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
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the new checkpoint (output parameter).
  /// \return ray::Status.
  ray::Status PrepareActorCheckpoint(const ActorID &actor_id,
                                     ActorCheckpointID &checkpoint_id);

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

  Language GetLanguage() const { return language_; }

  ClientID GetClientID() const { return client_id_; }

  JobID GetJobID() const { return job_id_; }

  bool IsWorker() const { return is_worker_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  const ClientID client_id_;
  const bool is_worker_;
  const JobID job_id_;
  const Language language_;
  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;
  /// The connection to the raylet server.
  std::unique_ptr<RayletConnection> conn_;
};

#endif
