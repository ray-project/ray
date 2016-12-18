#include "table.h"

#include <inttypes.h>
#include "redis.h"

void default_table_failure_callback(object_id id,
                                    void *user_context,
                                    void *user_data) {
  CHECKM(0, "default_table_failure_callback was called.");
}

static const retry_info default_retry = {
    .num_retries = 0,
    .timeout = 1000,
    .fail_callback = default_table_failure_callback};

table_callback_data *init_table_callback(db_handle *db_handle,
                                         unique_id id,
                                         const char *label,
                                         OWNER void *data,
                                         retry_info *retry,
                                         table_done_callback done_callback,
                                         table_retry_callback retry_callback,
                                         void *user_context) {
  CHECK(db_handle);
  CHECK(db_handle->loop);
  /* If no retry info is provided, use the default retry info. */
  if (retry == NULL) {
    retry = (retry_info *) &default_retry;
  }
  CHECK(retry);
  /* Allocate and initialize callback data structure for object table */
  table_callback_data *callback_data = malloc(sizeof(table_callback_data));
  CHECKM(callback_data != NULL, "Memory allocation error!")
  callback_data->id = id;
  callback_data->label = label;
  callback_data->retry = *retry;
  callback_data->done_callback = done_callback;
  callback_data->retry_callback = retry_callback;
  callback_data->data = data;
  callback_data->requests_info = NULL;
  callback_data->user_context = user_context;
  callback_data->db_handle = db_handle;
  /* Add timer and initialize it. */
  callback_data->timer_id = event_loop_add_timer(
      db_handle->loop, retry->timeout,
      (event_loop_timer_handler) table_timeout_handler, callback_data);
  outstanding_callbacks_add(callback_data);

  LOG_DEBUG("Initializing table command %s with timer ID %" PRId64,
            callback_data->label, callback_data->timer_id);
  callback_data->retry_callback(callback_data);

  return callback_data;
}

void destroy_timer_callback(event_loop *loop,
                            table_callback_data *callback_data) {
  event_loop_remove_timer(loop, callback_data->timer_id);
  destroy_table_callback(callback_data);
}

void destroy_table_callback(table_callback_data *callback_data) {
  CHECK(callback_data != NULL);

  if (callback_data->requests_info)
    free(callback_data->requests_info);

  if (callback_data->data) {
    free(callback_data->data);
    callback_data->data = NULL;
  }

  outstanding_callbacks_remove(callback_data);

  /* Timer is removed via EVENT_LOOP_TIMER_DONE in the timeout callback. */
  free(callback_data);
}

int64_t table_timeout_handler(event_loop *loop,
                              int64_t timer_id,
                              void *user_context) {
  CHECK(loop != NULL);
  CHECK(user_context != NULL);
  table_callback_data *callback_data = (table_callback_data *) user_context;

  CHECK(callback_data->retry.num_retries >= 0)
  LOG_DEBUG("retrying operation, retry_count = %d",
            callback_data->retry.num_retries);

  if (callback_data->retry.num_retries == 0) {
    /* We didn't get a response from the database after exhausting all retries;
     * let user know, cleanup the state, and remove the timer. */
    LOG_WARN("Table command %s with timer ID %" PRId64 " failed",
             callback_data->label, timer_id);
    if (callback_data->retry.fail_callback) {
      callback_data->retry.fail_callback(
          callback_data->id, callback_data->user_context, callback_data->data);
    }
    destroy_table_callback(callback_data);
    return EVENT_LOOP_TIMER_DONE;
  }

  /* Decrement retry count and try again. */
  callback_data->retry.num_retries--;
  callback_data->retry_callback(callback_data);
  return callback_data->retry.timeout;
}

/**
 * Hash table maintaining the outstanding callbacks.
 *
 * This hash table is used to handle the following case:
 * - a table command is issued with an associated callback and a callback data
 *   structure;
 * - the last timeout associated to this command expires, as a result the
 *   callback data structure is freed;
 * - a reply arrives, but now the callback data structure is gone, so we have
 *   to ignore this reply;
 *
 * This hash table enables us to ignore such replies. The operations on the
 * hash table are as follows.
 *
 * When we issue a table command and a timeout event to wait for the reply, we
 * add a new entry to the hash table that is keyed by the ID of the timer. Note
 * that table commands must have unique timer IDs, which are assigned by the
 * Redis ae event loop.
 *
 * When we receive the reply, we check whether the callback still exists in
 * this hash table, and if not we just ignore the reply. If the callback does
 * exist, the reply receiver is responsible for removing the timer and the
 * entry associated to the callback, or else the timeout handler will continue
 * firing.
 *
 * When the last timeout associated to the command expires we remove the entry
 * associated to the callback.
 */
static table_callback_data *outstanding_callbacks = NULL;

void outstanding_callbacks_add(table_callback_data *callback_data) {
  HASH_ADD_INT(outstanding_callbacks, timer_id, callback_data);
}

table_callback_data *outstanding_callbacks_find(int64_t key) {
  table_callback_data *callback_data = NULL;
  HASH_FIND_INT(outstanding_callbacks, &key, callback_data);
  return callback_data;
}

void outstanding_callbacks_remove(table_callback_data *callback_data) {
  HASH_DEL(outstanding_callbacks, callback_data);
}

void destroy_outstanding_callbacks(event_loop *loop) {
  table_callback_data *callback_data, *tmp;
  HASH_ITER(hh, outstanding_callbacks, callback_data, tmp) {
    destroy_timer_callback(loop, callback_data);
  }
}
