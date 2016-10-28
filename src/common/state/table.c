#include "table.h"

#include <inttypes.h>
#include "redis.h"

table_callback_data *init_table_callback(db_handle *db_handle,
                                         unique_id id,
                                         void *data,
                                         retry_info *retry,
                                         table_done_callback done_callback,
                                         table_retry_callback retry_callback,
                                         void *user_context) {
  CHECK(db_handle);
  CHECK(db_handle->loop);
  CHECK(retry);
  /* Allocate and initialize callback data structure for object table */
  table_callback_data *callback_data = malloc(sizeof(table_callback_data));
  CHECKM(callback_data != NULL, "Memory allocation error!")
  callback_data->id = id;
  callback_data->retry = *retry;
  callback_data->done_callback = done_callback;
  callback_data->retry_callback = retry_callback;
  callback_data->data = data;
  callback_data->requests_info = NULL;
  callback_data->user_context = user_context;
  callback_data->db_handle = db_handle;
  /* Add timer and initialize it. */
  callback_data->timer_id = event_loop_add_timer(
      db_handle->loop, retry->timeout, table_timeout_handler, callback_data);
  outstanding_callbacks_add(callback_data);

  callback_data->retry_callback(callback_data);

  return callback_data;
}

void destroy_table_callback(table_callback_data *callback_data) {
  CHECK(callback_data != NULL);

  // if (callback_data->data)
  //   free(callback_data->data);

  if (callback_data->requests_info)
    free(callback_data->requests_info);

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
    if (callback_data->retry.fail_callback) {
      callback_data->retry.fail_callback(callback_data->id,
                                         callback_data->user_context);
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
 * List of outstanding callbacks. We need to maintain this list for the case in
 * which a reply is received
 * after te last timeout expires and all relevant data structures are removed.
 * In this case we just need
 * to ignore the reply.
 * */
static outstanding_callback *outstanding_callbacks = NULL;

void outstanding_callbacks_add(table_callback_data *key) {
  outstanding_callback *callback = malloc(sizeof(outstanding_callback));

  CHECK(callback != NULL);
  callback->key = key;
  HASH_ADD_PTR(outstanding_callbacks, key, callback);
}

outstanding_callback *outstanding_callbacks_find(table_callback_data *key) {
  outstanding_callback *callback = NULL;

  HASH_FIND_PTR(outstanding_callbacks, &key, callback);
  return callback;
}

void outstanding_callbacks_remove(table_callback_data *key) {
  outstanding_callback *callback = NULL;

  callback = outstanding_callbacks_find(key);
  if (callback != NULL) {
    HASH_DEL(outstanding_callbacks, callback);
    free(callback);
  }
}
