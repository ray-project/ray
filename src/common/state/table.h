#ifndef TABLE_H
#define TABLE_H

#include "common.h"
#include "db.h"

typedef struct TableCallbackData TableCallbackData;

typedef void *table_done_callback;

/* The callback called when the database operation hasn't completed after
 * the number of retries specified for the operation.
 *
 * @param id The unique ID that identifies this callback. Examples include an
 *        object ID or task ID.
 * @param user_context The state context for the callback. This is equivalent
 *        to the user_context field in TableCallbackData.
 * @param user_data A data argument for the callback. This is equivalent to the
 *        data field in TableCallbackData. The user is responsible for
 *        freeing user_data.
 */
typedef void (*table_fail_callback)(UniqueID id,
                                    void *user_context,
                                    void *user_data);

typedef void (*table_retry_callback)(TableCallbackData *callback_data);

/**
 * Data structure consolidating the retry related variables. If a NULL
 * RetryInfo struct is used, the default behavior will be to retry infinitely
 * many times.
 */
typedef struct {
  /** Number of retries. This field will be decremented every time a retry
   *  occurs (unless the value is -1). If this value is -1, then there will be
   *  infinitely many retries. */
  int num_retries;
  /** Timeout, in milliseconds. */
  uint64_t timeout;
  /** The callback that will be called if there are no more retries left. */
  table_fail_callback fail_callback;
} RetryInfo;

struct TableCallbackData {
  /** ID of the entry in the table that we are going to look up, remove or add.
   */
  UniqueID id;
  /** A label to identify the original request for logging purposes. */
  const char *label;
  /** The callback that will be called when results is returned. */
  table_done_callback done_callback;
  /** The callback that will be called to initiate the next try. */
  table_retry_callback retry_callback;
  /** Retry information containing the remaining number of retries, the timeout
   *  before the next retry, and a pointer to the failure callback.
   */
  RetryInfo retry;
  /** Pointer to the data that is entered into the table. This can be used to
   *  pass the result of the call to the callback. The callback takes ownership
   *  over this data and will free it. */
  void *data;
  /** Pointer to the data used internally to handle multiple database requests.
   */
  void *requests_info;
  /** User context. */
  void *user_context;
  /** Handle to db. */
  DBHandle *db_handle;
  /** Handle to timer. */
  int64_t timer_id;
};

/**
 * Function to handle the timeout event.
 *
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
 * Initialize the table callback and call the retry_callback for the first time.
 *
 * @param db_handle Database handle.
 * @param id ID of the object that is looked up, added or removed.
 * @param label A string label to identify the type of table request for
 *        logging purposes.
 * @param data Data entered into the table. Shall be freed by the user.
 * @param retry Retry relevant information: retry timeout, number of remaining
 *        retries, and retry callback.
 * @param done_callback Function to be called when database returns result.
 * @param fail_callback Function to be called when number of retries is
 *        exhausted.
 * @param user_context Context that can be provided by the user and will be
 *        passed on to the various callbacks.
 * @return New table callback data struct.
 */
TableCallbackData *init_table_callback(DBHandle *db_handle,
                                       UniqueID id,
                                       const char *label,
                                       OWNER void *data,
                                       RetryInfo *retry,
                                       table_done_callback done_callback,
                                       table_retry_callback retry_callback,
                                       void *user_context);

/**
 * Destroy any state associated with the callback data. This removes all
 * associated state from the outstanding callbacks unordered map and frees any
 * associated memory. This does not remove any associated timer events.
 *
 * @param callback_data The pointer to the data structure of the callback we
 *        want to remove.
 * @return Void.
 */
void destroy_table_callback(TableCallbackData *callback_data);

/**
 * Destroy all state events associated with the callback data, including memory
 * and timer events.
 *
 * @param loop The event loop.
 * @param callback_data The pointer to the data structure of the callback we
 *        want to remove.
 * @return Void.
 */
void destroy_timer_callback(event_loop *loop, TableCallbackData *callback_data);

/**
 * Remove the callback timer without destroying the callback data.
 *
 * @param loop The event loop.
 * @param callback_data The pointer to the data structure of the callback.
 * @return Void.
 */
void remove_timer_callback(event_loop *loop, TableCallbackData *callback_data);

/**
 * Add an outstanding callback entry.
 *
 * @param callback_data The pointer to the data structure of the callback we
 *        want to insert.
 * @return None.
 */
void outstanding_callbacks_add(TableCallbackData *callback_data);

/**
 * Find an outstanding callback entry.
 *
 * @param key The key for the outstanding callbacks unordered map. We use the
 *        timer ID assigned by the Redis ae event loop.
 * @return Returns the callback data if found, NULL otherwise.
 */
TableCallbackData *outstanding_callbacks_find(int64_t key);

/**
 * Remove an outstanding callback entry. This only removes the callback entry
 * from the unordered map. It does not free the entry or remove any associated
 * timer events.
 *
 * @param callback_data The pointer to the data structure of the callback we
 *        want to remove.
 * @return Void.
 */
void outstanding_callbacks_remove(TableCallbackData *callback_data);

/**
 * Destroy all outstanding callbacks and remove their associated timer events
 * from the event loop.
 *
 * @param loop The event loop from which we want to remove the timer events.
 * @return Void.
 */
void destroy_outstanding_callbacks(event_loop *loop);

#endif /* TABLE_H */
