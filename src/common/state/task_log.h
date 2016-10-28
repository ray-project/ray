#ifndef TASK_LOG_H
#define TASK_LOG_H

#include "db.h"
#include "table.h"
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

/* Callback called when the task log operation completes. */
typedef void (*task_log_done_cb)(task_iid task_iid, void *user_context);

/*
 *  ==== Publish the task log ====
 */

/** Add or update a task instance to the task log.
 *  @param db_handle Database handle.
 *  @param retry_count Number of retries to the database before giving up.
 *  @param timeout Timout between retries (in milliseconds).
 *  @param done_cb Function to be called when database returns result.
 *  @param fail_cb Function to be called if we failed to contact
 *         database after retry_count retries.
 *  @param user_context Data that will be passed to done_cb and fail_cb.
 *  @return Void.
 */
void task_log_publish(db_handle *db_handle,
                      task_instance *task_instance,
                      int retry_count,
                      uint64_t timeout,
                      task_log_done_cb done_cb,
                      table_fail_cb fail_cb,
                      void *user_context);

/*
 *  ==== Subscribing to the task log ====
 */

/* Callback for subscribing to the task log. */
typedef void (*task_log_subscribe_cb)(task_instance *task_instance,
                                      void *user_context);

/** Register callback for a certain event.
 *
 *  @param db_handle Database handle.
 *  @param subscribe_cb Callback that will be called when the task log is
           updated.
 *  @param subscribe_context Context that will be passed into the subscribe_cb.
 *  @param node Node whose events we want to listen to. If you want to register
 *         to updates from all nodes, set node = NIL_ID.
 *  @param state_filter Flags for events we want to listen to. If you want
 *         to listen to all events, use state_filter = TASK_WAITING |
 *         TASK_SCHEDULED | TASK_RUNNING | TASK_DONE.
 *  @param retry_count Number of retries to the database before giving up.
 *  @param timeout Timout between retries (in milliseconds).
 *  @param done_cb Function to be called when database returns result.
 *  @param fail_cb Function to be called if we failed to contact
 *         database after retry_count retries.
 *  @param user_context Data that will be passed to done_cb and fail_cb.
 *  @return Void.
 */
void task_log_subscribe(db_handle *db_handle,
                        node_id node,
                        int32_t state_filter,
                        task_log_subscribe_cb subscribe_cb,
                        void *subscribe_context,
                        int retry_count,
                        uint64_t timeout,
                        task_log_done_cb done_cb,
                        table_fail_cb fail_cb,
                        void *user_context);

/* Data that is needed to register task log subscribe callbacks with the state
 * database. */
typedef struct {
  node_id node;
  int32_t state_filter;
  task_log_subscribe_cb subscribe_cb;
  void *subscribe_context;
} task_log_subscribe_data;

#endif /* TASK_LOG_H */
