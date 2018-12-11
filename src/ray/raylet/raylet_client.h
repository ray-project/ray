#ifndef RAYLET_CLIENT_H
#define RAYLET_CLIENT_H

#include <unistd.h>
#include <mutex>

#include "ray/raylet/task_spec.h"
#include "ray/status.h"

using ray::ActorID;
using ray::JobID;
using ray::ObjectID;
using ray::TaskID;
using ray::UniqueID;

using MessageType = ray::protocol::MessageType;

class RayletConnection {
 public:
  /// Connect to the local scheduler.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the
  ///        local scheduler.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  ///        additional message will be sent to register as one.
  /// \param driver_id The ID of the driver. This is non-nil if the client is a
  ///        driver.
  /// \return The connection information.
  RayletConnection(const std::string &raylet_socket, int num_retries, int64_t timeout);

  ~RayletConnection() { close(conn_); }
  /// Notify the local scheduler that this client is disconnecting gracefully. This
  /// is used by actors to exit gracefully so that the local scheduler doesn't
  /// propagate an error message to the driver.
  ///
  /// \return ray::Status.
  ray::Status Disconnect();
  ray::Status ReadMessage(MessageType type, uint8_t *&message);
  ray::Status WriteMessage(MessageType type,
                           flatbuffers::FlatBufferBuilder *fbb = nullptr);
  ray::Status AtomicRequestReply(MessageType request_type, MessageType reply_type,
                                 uint8_t *&reply_message,
                                 flatbuffers::FlatBufferBuilder *fbb = nullptr);

 private:
  // TODO(rkn): The io methods below should be removed.
  int connect_ipc_sock(const std::string &socket_pathname);
  int read_bytes(uint8_t *cursor, size_t length);
  int write_bytes(uint8_t *cursor, size_t length);

  /// File descriptor of the Unix domain socket that connects to raylet.
  int conn_;
  /// A mutex to protect stateful operations of the raylet client.
  std::mutex mutex_;
  /// A mutex to protect write operations of the local scheduler client.
  std::mutex write_mutex_;
};

class RayletClient {
 public:
  /// Connect to the local scheduler.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  /// additional message will be sent to register as one.
  /// \param driver_id The ID of the driver. This is non-nil if the client is a driver.
  /// \return The connection information.
  RayletClient(const std::string &raylet_socket, const UniqueID &client_id,
               bool is_worker, const JobID &driver_id, const Language &language);

  ray::Status Disconnect() { return conn_->Disconnect(); };

  /// Submit a task using the raylet code path.
  ///
  /// \param The execution dependencies.
  /// \param The task specification.
  /// \return ray::Status.
  ray::Status SubmitTask(const std::vector<ObjectID> &execution_dependencies,
                         const ray::raylet::TaskSpecification &task_spec);

  /// Get next task for this client. This will block until the scheduler assigns
  /// a task to this worker. The caller takes ownership of the returned task
  /// specification and must free it.
  ///
  /// \return The assigned task.
  ray::raylet::TaskSpecification *GetTask();

  /// Tell the local scheduler that the client has finished executing a task.
  ///
  /// \return ray::Status.
  ray::Status TaskDone();

  /// Tell the local scheduler to reconstruct or fetch objects.
  ///
  /// \param object_ids The IDs of the objects to reconstruct.
  /// \param fetch_only Only fetch objects, do not reconstruct them.
  /// \param current_task_id The task that needs the objects.
  /// \return int 0 means correct, other numbers mean error.
  ray::Status FetchOrReconstruct(const std::vector<ObjectID> &object_ids, bool fetch_only,
                                 const TaskID &current_task_id);
  /// Notify the local scheduler that this client (worker) is no longer blocked.
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
  /// \return A pair with the first element containing the object ids that were
  /// found, and the second element the objects that were not found.
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> Wait(
      const std::vector<ObjectID> &object_ids, int num_returns,
      int64_t timeout_milliseconds, bool wait_local, const TaskID &current_task_id);

  /// Push an error to the relevant driver.
  ///
  /// \param The ID of the job that the error is for.
  /// \param The type of the error.
  /// \param The error message.
  /// \param The timestamp of the error.
  /// \return ray::Status.
  ray::Status PushError(const JobID &job_id, const std::string &type,
                        const std::string &error_message, double timestamp);

  /// Store some profile events in the GCS.
  ///
  /// \param profile_events A batch of profiling event information.
  /// \return ray::Status.
  ray::Status PushProfileEvents(const ProfileTableDataT &profile_events);

  /// Free a list of objects from object stores.
  ///
  /// \param object_ids A list of ObjectsIDs to be deleted.
  /// \param local_only Whether keep this request with local object store
  /// or send it to all the object stores.
  /// \return ray::Status.
  ray::Status FreeObjects(const std::vector<ray::ObjectID> &object_ids, bool local_only);

  UniqueID client_id;
  bool is_worker;
  JobID driver_id;
  Language language;
  /// The IDs of the GPUs that this client can use.
  /// NOTE(rkn): This is only used by legacy Ray and will be deprecated.
  std::vector<int> gpu_ids;
  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>> resource_ids_;

 private:
  /// The connection to the raylet server.
  std::unique_ptr<RayletConnection> conn_;
};

#endif
