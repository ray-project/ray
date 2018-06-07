#ifndef LOCAL_SCHEDULER_CLIENT_H
#define LOCAL_SCHEDULER_CLIENT_H

#include "common/task.h"
#include "local_scheduler_shared.h"
#include "ray/raylet/task_spec.h"

struct LocalSchedulerConnection {
  /** File descriptor of the Unix domain socket that connects to local
   *  scheduler. */
  int conn;
  /** The IDs of the GPUs that this client can use. */
  std::vector<int> gpu_ids;
};

/**
 * Connect to the local scheduler.
 *
 * @param local_scheduler_socket The name of the socket to use to connect to the
 *        local scheduler.
 * @param is_worker Whether this client is a worker. If it is a worker, an
 *        additional message will be sent to register as one.
 * @return The connection information.
 */
LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    UniqueID worker_id,
    bool is_worker);

/**
 * Disconnect from the local scheduler.
 *
 * @param conn Local scheduler connection information returned by
 *        LocalSchedulerConnection_init.
 * @return Void.
 */
void LocalSchedulerConnection_free(LocalSchedulerConnection *conn);

/**
 * Submit a task to the local scheduler.
 *
 * @param conn The connection information.
 * @param execution_spec The execution spec for the task to submit.
 * @return Void.
 */
void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskExecutionSpec &execution_spec);

/// Submit a task using the raylet code path.
///
/// \param The connection information.
/// \param The execution dependencies.
/// \param The task specification.
/// \return Void.
void local_scheduler_submit_raylet(
    LocalSchedulerConnection *conn,
    const std::vector<ObjectID> &execution_dependencies,
    ray::raylet::TaskSpecification task_spec);

/**
 * Notify the local scheduler that this client is disconnecting gracefully. This
 * is used by actors to exit gracefully so that the local scheduler doesn't
 * propagate an error message to the driver.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_disconnect_client(LocalSchedulerConnection *conn);

/**
 * Log an event to the event log. This will call RPUSH key value. We use RPUSH
 * instead of SET so that it is possible to flush the log multiple times with
 * the same key (for example the key might be shared across logging calls in the
 * same task on a worker).
 *
 * @param conn The connection information.
 * @param key The key to store the event in.
 * @param key_length The length of the key.
 * @param value The value to store.
 * @param value_length The length of the value.
 * @param timestamp The time that the event is logged.
 * @return Void.
 */
void local_scheduler_log_event(LocalSchedulerConnection *conn,
                               uint8_t *key,
                               int64_t key_length,
                               uint8_t *value,
                               int64_t value_length,
                               double timestamp);

/**
 * Get next task for this client. This will block until the scheduler assigns
 * a task to this worker. This allocates and returns a task, and so the task
 * must be freed by the caller.
 *
 * @todo When does this actually get freed?
 *
 * @param conn The connection information.
 * @param task_size A pointer to fill out with the task size.
 * @return The address of the assigned task.
 */
TaskSpec *local_scheduler_get_task(LocalSchedulerConnection *conn,
                                   int64_t *task_size);

/**
 * Tell the local scheduler that the client has finished executing a task.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_task_done(LocalSchedulerConnection *conn);

/**
 * Tell the local scheduler to reconstruct an object.
 *
 * @param conn The connection information.
 * @param object_id The ID of the object to reconstruct.
 * @return Void.
 */
void local_scheduler_reconstruct_object(LocalSchedulerConnection *conn,
                                        ObjectID object_id);

/**
 * Send a log message to the local scheduler.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_log_message(LocalSchedulerConnection *conn);

/**
 * Notify the local scheduler that this client (worker) is no longer blocked.
 *
 * @param conn The connection information.
 * @return Void.
 */
void local_scheduler_notify_unblocked(LocalSchedulerConnection *conn);

/**
 * Record the mapping from object ID to task ID for put events.
 *
 * @param conn The connection information.
 * @param task_id The ID of the task that called put.
 * @param object_id The ID of the object being stored.
 * @return Void.
 */
void local_scheduler_put_object(LocalSchedulerConnection *conn,
                                TaskID task_id,
                                ObjectID object_id);

/**
 * Get an actor's current task frontier.
 *
 * @param conn The connection information.
 * @param actor_id The ID of the actor whose frontier is returned.
 * @return A byte vector that can be traversed as an ActorFrontier flatbuffer.
 */
const std::vector<uint8_t> local_scheduler_get_actor_frontier(
    LocalSchedulerConnection *conn,
    ActorID actor_id);

/**
 * Set an actor's current task frontier.
 *
 * @param conn The connection information.
 * @param frontier An ActorFrontier flatbuffer to set the frontier to.
 * @return Void.
 */
void local_scheduler_set_actor_frontier(LocalSchedulerConnection *conn,
                                        const std::vector<uint8_t> &frontier);

/// Wait for the given objects until timeout expires or num_return objects are
/// found.
///
/// \param conn The connection information.
/// \param object_ids The objects to wait for.
/// \param num_returns The number of objects to wait for.
/// \param timeout_milliseconds Duration, in milliseconds, to wait before
/// returning.
/// \param wait_local Whether to wait for objects to appear on this node.
/// \return A pair with the first element containing the object ids that were
/// found, and the second element the objects that were not found.
std::pair<std::vector<ObjectID>, std::vector<ObjectID>> local_scheduler_wait(
    LocalSchedulerConnection *conn,
    const std::vector<ObjectID> &object_ids,
    int num_returns,
    int64_t timeout_milliseconds,
    bool wait_local);

#endif
