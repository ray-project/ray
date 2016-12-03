#ifndef task_table_H
#define task_table_H

#include "db.h"
#include "table.h"
#include "task.h"

/**
 * The task table is a message bus that is used for all communication between
 * local and global schedulers (and also persisted to the state database).
 * Here are examples of events that are recorded by the task table:
 *
 * 1) local scheduler writes when it submits a task to the global scheduler;
 * 2) global scheduler reads it to get the task submitted by local schedulers;
 * 3) global scheduler writes it when assigning the task to a local scheduler;
 * 4) local scheduler reads it to get its tasks assigned by global scheduler;
 * 5) local scheduler writes it when a task finishes execution;
 * 6) global scheduler reads it to get the tasks that have finished; */

/* Callback called when a task table write operation completes. */
typedef void (*task_table_done_callback)(task_id task_id, void *user_context);

/* Callback called when a task table read operation completes. */
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
 * Add a task entry, including task spec and scheduling information, to the
 * task table. This will overwrite any task already in the task table with the
 * same task ID.
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

/*
 *  ==== Subscribing to the task table ====
 */

/* Callback for subscribing to the task table. */
typedef void (*task_table_subscribe_callback)(task *task, void *user_context);

/**
 * Register a callback for a task event. An event is any update of a task in
 * the task table, produced by task_table_add_task or task_table_add_task.
 * Events include changes to the task's scheduling state or changes to the
 * task's node location.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the task table is
 *        updated.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param node Node whose events we want to listen to. If you want to register
 *        to updates from all nodes, set node = NIL_ID.
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
                          node_id node,
                          scheduling_state state_filter,
                          task_table_subscribe_callback subscribe_callback,
                          void *subscribe_context,
                          retry_info *retry,
                          task_table_done_callback done_callback,
                          void *user_context);

/* Data that is needed to register task table subscribe callbacks with the state
 * database. */
typedef struct {
  node_id node;
  scheduling_state state_filter;
  task_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} task_table_subscribe_data;

#endif /* task_table_H */
