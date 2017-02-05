#ifndef task_table_H
#define task_table_H

#include "db.h"
#include "table.h"
#include "task.h"

/**
 * The task table is a message bus that is used for communication between local
 * and global schedulers (and also persisted to the state database). Here are
 * examples of events that are recorded by the task table:
 *
 * 1) Local schedulers write to it when submitting a task to the global
 *    scheduler.
 * 2) The global scheduler subscribes to updates to the task table to get tasks
 *    submitted by local schedulers.
 * 3) The global scheduler writes to it when assigning a task to a local
 *    scheduler.
 * 4) Local schedulers subscribe to updates to the task table to get tasks
 *    assigned to them by the global scheduler.
 * 5) Local schedulers write to it when a task finishes execution.
 */

/* Callback called when a task table write operation completes. */
typedef void (*task_table_done_callback)(task_id task_id, void *user_context);

/* Callback called when a task table read operation completes. If the task ID
 * was not in the task table, then the task pointer will be NULL. */
typedef void (*task_table_get_callback)(task *task, void *user_context);

/**
 * Get a task's entry from the task table.
 *
 * @param db_handle Database handle.
 * @param task_id The ID of the task we want to look up.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void task_table_get_task(db_handle *db,
                         task_id task_id,
                         retry_info *retry,
                         task_table_get_callback done_callback,
                         void *user_context);

/**
 * Add a task entry, including task spec and scheduling information, to the task
 * table. This will overwrite any task already in the task table with the same
 * task ID.
 *
 * @param db_handle Database handle.
 * @param task The task entry to add to the table.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void task_table_add_task(db_handle *db_handle,
                         OWNER task *task,
                         retry_info *retry,
                         task_table_done_callback done_callback,
                         void *user_context);

/*
 *  ==== Publish the task table ====
 */

/**
 * Update a task's scheduling information in the task table. This assumes that
 * the task spec already exists in the task table entry.
 *
 * @param db_handle Database handle.
 * @param task The task entry to add to the table. The task spec in the entry is
 *        ignored.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void task_table_update(db_handle *db_handle,
                       OWNER task *task,
                       retry_info *retry,
                       task_table_done_callback done_callback,
                       void *user_context);

/**
 * Update a task's scheduling information in the task table, if the current
 * value matches the given test value. If the update succeeds, it also updates
 * the task entry's local scheduler ID with the ID of the client who called
 * this function. This assumes that the task spec already exists in the task
 * table entry.
 *
 * @param db_handle Database handle.
 * @param task_id The task ID of the task entry to update.
 * @param test_state The value to test the current task entry's scheduling
 *        state against.
 * @param update_state The value to update the task entry's scheduling state
 *        with, if the current state matches test_state.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void task_table_test_and_update(db_handle *db_handle,
                                task_id task_id,
                                int test_state,
                                int update_state,
                                retry_info *retry,
                                task_table_get_callback done_callback,
                                void *user_context);

/* Data that is needed to test and set the task's scheduling state. */
typedef struct {
  int test_state;
  int update_state;
  db_client_id local_scheduler_id;
} task_table_test_and_update_data;

/*
 *  ==== Subscribing to the task table ====
 */

/* Callback for subscribing to the task table. */
typedef void (*task_table_subscribe_callback)(task *task, void *user_context);

/**
 * Register a callback for a task event. An event is any update of a task in
 * the task table, produced by task_table_add_task or task_table_add_task.
 * Events include changes to the task's scheduling state or changes to the
 * task's local scheduler ID.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the task table is
 *        updated.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param local_scheduler_id The db_client_id of the local scheduler whose
 *        events we want to listen to. If you want to subscribe to updates from
 *        all local schedulers, pass in NIL_ID.
 * @param state_filter Flags for events we want to listen to. If you want
 *        to listen to all events, use state_filter = TASK_WAITING |
 *        TASK_SCHEDULED | TASK_RUNNING | TASK_DONE.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void task_table_subscribe(db_handle *db_handle,
                          db_client_id local_scheduler_id,
                          int state_filter,
                          task_table_subscribe_callback subscribe_callback,
                          void *subscribe_context,
                          retry_info *retry,
                          task_table_done_callback done_callback,
                          void *user_context);

/* Data that is needed to register task table subscribe callbacks with the state
 * database. */
typedef struct {
  db_client_id local_scheduler_id;
  int state_filter;
  task_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} task_table_subscribe_data;

#endif /* task_table_H */
