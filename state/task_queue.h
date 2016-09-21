#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include "db.h"
#include "task.h"

/* The task ID is a deterministic hash of the function ID that
 * the task executes and the argument IDs or argument values */
typedef unique_id task_id;

/* The task instance ID is a globally unique ID generated which
 * identifies this particular execution of the task */
typedef unique_id task_iid;

/* The node id is an identifier for the node the task is
 * scheduled on */
typedef unique_id node_id;

/* Callback for subscribing to the task queue. The only argument this
 * callback gets is the task_id of the. */
typedef void (*task_queue_callback)(task_iid *task_iid, task_spec *task);

/* Submit task to the global scheduler. */
void task_queue_submit_task(db_conn *db, task_iid task_iid, task_spec *task);

/* Submit task to a local scheduler based on the decision made by the global
 * scheduler. */
void task_queue_schedule_task(db_conn *db, task_iid task_iid, node_id node);

/* Subscribe to task queue. */
void task_queue_register_callback(db_conn *db, task_queue_callback callback);

#endif
