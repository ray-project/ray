#ifndef LOCAL_SCHEDULER_CLIENT_H
#define LOCAL_SCHEDULER_CLIENT_H

#include "common/task.h"
#include "local_scheduler_shared.h"

typedef struct {
  /** File descriptor of the Unix domain socket that connects to local
   *  scheduler. */
  int conn;
} LocalSchedulerConnection;

/**
 * Connect to the local scheduler.
 *
 * @param local_scheduler_socket The name of the socket to use to connect to the
 *        local scheduler.
 * @param actor_id The ID of the actor running on this worker. If no actor is
 *        running on this actor, this should be NIL_ACTOR_ID.
 * @param is_worker Whether this client is a worker. If it is a worker, an
 *        additional message will be sent to register as one.
 * @return The connection information.
 */
LocalSchedulerConnection *LocalSchedulerConnection_init(
    const char *local_scheduler_socket,
    ActorID actor_id,
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
 * @param task The address of the task to submit.
 * @return Void.
 */
void local_scheduler_submit(LocalSchedulerConnection *conn,
                            TaskSpec *task,
                            int64_t task_size);

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
 * @return Void.
 */
void local_scheduler_log_event(LocalSchedulerConnection *conn,
                               uint8_t *key,
                               int64_t key_length,
                               uint8_t *value,
                               int64_t value_length);

/**
 * Get next task for this client. This will block until the scheduler assigns
 * a task to this worker. This allocates and returns a task, and so the task
 * must be freed by the caller.
 *
 * @todo When does this actually get freed?
 *
 * @param conn The connection information.
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

#endif
