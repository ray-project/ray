#ifndef TABLE_H
#define TABLE_H

#include "uthash.h"
#include "stdbool.h"

#include "common.h"
#include "db.h"

typedef struct table_callback_data table_callback_data;

typedef void *table_done_cb;

/* The callback called when the database operation hasn't completed after
 * the number of retries specified for the operation. */
typedef void (*table_fail_cb)(unique_id id, void *user_context);

typedef void (*table_retry_cb)(table_callback_data *cb_data);

/**
 * Data structure consolidating the retry related varaibles.
 */
typedef struct {
  /** Number of retries left. */
  int num_retries;
  /** Timeout, in milliseconds. */
  uint64_t timeout;
  /** The callback that will be called if there are no more retries left. */
  table_fail_cb fail_cb;
} retry_info;

struct table_callback_data {
  /** ID of the entry in the table that we are going to look up, remove or add.
   */
  unique_id id;
  /** The callback that will be called when results is returned. */
  table_done_cb done_cb;
  /** The callback that will be called to initiate the next try. */
  table_retry_cb retry_cb;
  /** Retry information containing the remaining number of retries, the timeout
   *  before the next retry, and a pointer to the failure callback.
   */
  retry_info retry;
  /** Pointer to the data that is entered into the table. */
  void *data;
  /** Pointer to the data used internally to handle multiple database requests.
   */
  void *requests_info;
  /** User context. */
  void *user_context;
  /** Handle to db. */
  db_handle *db_handle;
  /** Handle to timer. */
  int64_t timer_id;
};

/**
 * Function to handle the timeout event.
 * @param loop Event loop.
 * @param timer_id Timer identifier.
 * @param context Pointer to the callback data for the object table
 * @return Timeout to reset the timer if we need to try again, or
 *         EVENT_LOOP_TIMER_DONE if retry_count == 0.
 */
int64_t table_timeout_handler(event_loop *loop,
                              int64_t timer_id,
                              void *context);

/**
 *
 * @param db_handle Database handle.
 * @param id ID of the object that is looked up, added or removed.
 * @param data Data entered into the table.
 * @param retry Retry relevant information: retry timeout, number of remaining
 * retries, and retry callback.
 * @param done_cb Function to be called when database returns result.
 * @param fail_cb Function to be called when number of retries is exhausted.
 * @param user_context Context that can be provided by the user and will be
 *        passed on to the various callbacks.
 * @return New table callback data struct.
 */
table_callback_data *init_table_callback(db_handle *db_handle,
                                         unique_id id,
                                         void *data,
                                         retry_info *retry,
                                         table_done_cb done_cb,
                                         table_retry_cb retry_cb,
                                         void *user_context);

void destroy_table_callback(table_callback_data *cb_data);

/**
 * Hash table maintaining the outstanding callbacks.
 *
 * This hash table is used to handle the following case:
 * - a table command is issued with an associated callback and a callback data
 * structure;
 * - the last timeout associated to this command expires, as a result the
 * callback data structure is freed;
 * - a reply arrives, but now the callback data structure is gone, so we have to
 * ignore this reply;
 *
 * This hash table enables us to ignore such replies. The operations on the hash
 * table are as follows.
 *
 * When we issue a table command we add a new entry to the hash table that is
 * keyed by the address of the callback's
 * data structure.
 *
 * When we receive the reply, we check whether the callback still exists in this
 * hash table, and if not we just ignore
 * the reply.
 *
 * When the last timeout associated to the command expires we remove the entry
 * associated to the collback.
 */
typedef struct {
  table_callback_data *key;
  int dummy;
  UT_hash_handle hh; /* makes this structure hashable */
} outstanding_callback;

/**
 *
 * @param key The pointer to the data structure of the callback we want to
 * insert.
 * @return None.
 */
void outstanding_callbacks_add(table_callback_data *key);

/**
 *
 * @param key The pointer to the data structure of the callback we are looking
 * for.
 * @return Returns callback if found, NULL otherwise.
 */
outstanding_callback *outstanding_callbacks_find(table_callback_data *key);

/**
 *
 * @param key Key, defined as the pointer to the data structure of the callback
 * we want to remove.
 * @return None.
 */
void outstanding_callbacks_remove(table_callback_data *key);

#endif /* TABLE_H */
