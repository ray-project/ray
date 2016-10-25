#ifndef TASK_LOG_H
#define TASK_LOG_H

#include "db.h"
#include "task.h"

/* The task log is a message bus that is used for all communication between
 * local and global schedulers (and also persisted to the state database).
 * Here are examples of events that are recorded by the task log:
 *
 * 1) local scheduler writes it when submits a task to the global scheduler;
 * 2) global scheduler reads it to get the task submitted by local schedulers;
 * 3) global scheduler writes it when assigning the task to a local scheduler;
 * 4) local scheduler reads it to get its tasks assigned by global scheduler;
 * 5) local scheduler writes it when a task finishes execution;
 * 6) global scheduler reads it to get the tasks that have finished; */

/* Callback for subscribing to the task log. */
typedef void (*task_log_callback)(task_instance *task_instance, void *userdata);

/* Initially add a task instance to the task log. */
void task_log_add_task(db_handle *db, task_instance *task_instance);

/* Update task instance in the task log. */
void task_log_update_task(db_handle *db,
                          task_iid task_iid,
                          int32_t state,
                          node_id node);

/* Register callback for a certain event. The node specifies the node whose
 * events we want to listen to. If you want to listen to all events for this
 * node, use state_filter =
 *     TASK_WAITING | TASK_SCHEDULED | TASK_RUNNING | TASK_DONE.
 * If you want to register to updates from all nodes, set node = NIL_ID. */
void task_log_register_callback(db_handle *db,
                                task_log_callback callback,
                                node_id node,
                                int32_t state_filter,
                                void *userdata);

#endif /* TASK_LOG_H */
