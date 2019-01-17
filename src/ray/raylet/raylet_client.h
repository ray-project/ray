#ifndef RAYLET_CLIENT_H
#define RAYLET_CLIENT_H

#include <unistd.h>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "ray/common/client_connection.h"
#include "ray/raylet/task_spec.h"
#include "ray/status.h"

using ray::ActorID;
using ray::ClientID;
using ray::JobID;
using ray::ObjectID;
using ray::TaskID;
using ray::UniqueID;

using ray::protocol::MessageType;
using ResourceMappingType =
    std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>>;
using WaitResultPair = std::pair<std::vector<ObjectID>, std::vector<ObjectID>>;

class RayletClient {
 public:
  /// Connect to the raylet.
  ///
  /// \param raylet_socket The name of the socket to use to connect to the raylet.
  /// \param worker_id A unique ID to represent the worker.
  /// \param is_worker Whether this client is a worker. If it is a worker, an
  /// additional message will be sent to register as one.
  /// \param driver_id The ID of the driver. This is non-nil if the client is a driver.
  /// \return The connection information.
  RayletClient(const std::string &raylet_socket, const UniqueID &client_id,
               bool is_worker, const JobID &driver_id, const Language &language);

  /// Disconnect from raylet server.
  ///
  /// \return ray::Status.
  ray::Status Disconnect();

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
  /// \param task_spec The assigned task.
  /// \return ray::Status.
  ray::Status GetTask(std::unique_ptr<ray::raylet::TaskSpecification> *task_spec);

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

  Language GetLanguage() const { return language_; }

  JobID GetClientID() const { return client_id_; }

  JobID GetDriverID() const { return driver_id_; }

  bool IsWorker() const { return is_worker_; }

  const ResourceMappingType &GetResourceIDs() const { return resource_ids_; }

 private:
  /// The ID for this raylet client.
  const ClientID client_id_;
  /// Whether this client belongs to a worker.
  const bool is_worker_;
  /// The TaskID for the driver.
  const JobID driver_id_;
  /// The programming language using this C++ client.
  const Language language_;
  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  ResourceMappingType resource_ids_;
  /// The Boost IO service.
  boost::asio::io_service main_service_;
  /// The connection to the raylet server.
  std::unique_ptr<ray::LocalThreadSafeConnection> conn_;
};

#endif
