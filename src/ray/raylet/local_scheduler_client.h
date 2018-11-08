#ifndef LOCAL_SCHEDULER_CLIENT_H
#define LOCAL_SCHEDULER_CLIENT_H

#include <mutex>

#include "ray/raylet/task_spec.h"

using ray::ObjectID;
using ray::JobID;
using ray::TaskID;
using ray::ActorID;
using ray::UniqueID;

struct LocalSchedulerConnection {
  /** File descriptor of the Unix domain socket that connects to local
   *  scheduler. */
  int conn;
  /** The IDs of the GPUs that this client can use. NOTE(rkn): This is only used
   *  by legacy Ray and will be deprecated. */
  std::vector<int> gpu_ids;
  /// A map from resource name to the resource IDs that are currently reserved
  /// for this worker. Each pair consists of the resource ID and the fraction
  /// of that resource allocated for this worker.
  std::unordered_map<std::string, std::vector<std::pair<int64_t, double>>> resource_ids_;
  /// A mutex to protect stateful operations of the local scheduler client.
  std::mutex mutex;
  /// A mutext to protect write operations of the local scheduler client.
  std::mutex write_mutex;
};

/**
 * Connect to the local scheduler.
 *
 * @param local_scheduler_socket The name of the socket to use to connect to the
 *        local scheduler.
 * @param worker_id A unique ID to represent the worker.
 * @param is_worker Whether this client is a worker. If it is a worker, an
 *        additional message will be sent to register as one.
 * @param driver_id The ID of the driver. This is non-nil if the client is a
 *        driver.
 * @return The connection information.
 */
LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket, const UniqueID &worker_id, bool is_worker,
    const JobID &driver_id, const Language &language);

/**
 * Disconnect from the local scheduler.
 *
 * @param conn Local scheduler connection information returned by
 *        LocalSchedulerConnection_init.
 * @return Void.
 */
void LocalSchedulerConnection_free(LocalSchedulerConnection *conn);

/// Submit a task using the raylet code path.
///
/// \param The connection information.
/// \param The execution dependencies.
/// \param The task specification.
/// \return Void.
void local_scheduler_submit_raylet(LocalSchedulerConnection *conn,
                                   const std::vector<ObjectID> &execution_dependencies,
                                   const ray::raylet::TaskSpecification &task_spec);

/**
 * Notify the local scheduler that this client is disconnecting gracefully. This
 * is used by actors to exit gracefully so that the local scheduler doesn't
 * propagate an error message to the driver.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_disconnect_client(LocalSchedulerConnection *conn);

/// Get next task for this client. This will block until the scheduler assigns
/// a task to this worker. The caller takes ownership of the returned task
/// specification and must free it.
///
/// \param conn The connection information.
/// \return The assigned task.
ray::raylet::TaskSpecification *local_scheduler_get_task_raylet(
    LocalSchedulerConnection *conn);

/**
 * Tell the local scheduler that the client has finished executing a task.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_task_done(LocalSchedulerConnection *conn);

/**
 * Tell the local scheduler to reconstruct or fetch objects.
 *
 * @param conn The connection information.
 * @param object_ids The IDs of the objects to reconstruct.
 * @param fetch_only Only fetch objects, do not reconstruct them.
 * @param current_task_id The task that needs the objects.
 * @return Void.
 */
void local_scheduler_fetch_or_reconstruct(LocalSchedulerConnection *conn,
                                          const std::vector<ObjectID> &object_ids,
                                          bool fetch_only, const TaskID &current_task_id);

/**
 * Notify the local scheduler that this client (worker) is no longer blocked.
 *
 * @param conn The connection information.
 * @param current_task_id The task that is no longer blocked.
 * @return Void.
 */
void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn,
                                      const TaskID &current_task_id);

// /**
//  * Get an actor's current task frontier.
//  *
//  * @param conn The connection information.
//  * @param actor_id The ID of the actor whose frontier is returned.
//  * @return A byte vector that can be traversed as an ActorFrontier flatbuffer.
//  */
// const std::vector<uint8_t> local_scheduler_get_actor_frontier(
//     LocalSchedulerConnection *conn,
//     ActorID actor_id);

// /**
//  * Set an actor's current task frontier.
//  *
//  * @param conn The connection information.
//  * @param frontier An ActorFrontier flatbuffer to set the frontier to.
//  * @return Void.
//  */
// void local_scheduler_set_actor_frontier(LocalSchedulerConnection *conn,
//                                         const std::vector<uint8_t> &frontier);

/// Wait for the given objects until timeout expires or num_return objects are
/// found.
///
/// \param conn The connection information.
/// \param object_ids The objects to wait for.
/// \param num_returns The number of objects to wait for.
/// \param timeout_milliseconds Duration, in milliseconds, to wait before
/// returning.
/// \param wait_local Whether to wait for objects to appear on this node.
/// \param current_task_id The task that called wait.
/// \return A pair with the first element containing the object ids that were
/// found, and the second element the objects that were not found.
std::pair<std::vector<ObjectID>, std::vector<ObjectID>> local_scheduler_wait(
    LocalSchedulerConnection *conn, const std::vector<ObjectID> &object_ids,
    int num_returns, int64_t timeout_milliseconds, bool wait_local,
    const TaskID &current_task_id);

/// Push an error to the relevant driver.
///
/// \param conn The connection information.
/// \param The ID of the job that the error is for.
/// \param The type of the error.
/// \param The error message.
/// \param The timestamp of the error.
/// \return Void.
void local_scheduler_push_error(LocalSchedulerConnection *conn, const JobID &job_id,
                                const std::string &type, const std::string &error_message,
                                double timestamp);

/// Store some profile events in the GCS.
///
/// \param conn The connection information.
/// \param profile_events A batch of profiling event information.
/// \return Void.
void local_scheduler_push_profile_events(LocalSchedulerConnection *conn,
                                         const ProfileTableDataT &profile_events);

/// Free a list of objects from object stores.
///
/// \param conn The connection information.
/// \param object_ids A list of ObjectsIDs to be deleted.
/// \param local_only Whether keep this request with local object store
/// or send it to all the object stores.
/// \return Void.
void local_scheduler_free_objects_in_object_store(
    LocalSchedulerConnection *conn, const std::vector<ray::ObjectID> &object_ids,
    bool local_only);

#endif
