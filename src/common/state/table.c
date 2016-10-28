#include "table.h"

#include <inttypes.h>
#include "redis.h"

table_callback_data *init_table_callback(db_handle *db_handle,
                                         unique_id id,
                                         void *data,
                                         retry_struct *retry,
                                         table_done_cb done_cb,
                                         table_fail_cb fail_cb,
                                         void *user_context) {
  CHECK(db_handle);
  CHECK(db_handle->loop);
  CHECK(retry);
  /* Allocate and initialize callback data structure for object table */
  table_callback_data *cb_data = malloc(sizeof(table_callback_data));
  CHECKM(cb_data != NULL, "Memory allocation error!")
  cb_data->id = id;
  cb_data->done_cb = done_cb;
  cb_data->fail_cb = fail_cb;
  cb_data->retry.cb = retry->cb;
  cb_data->retry.count = retry->count;
  cb_data->retry.timeout = retry->timeout;
  cb_data->data = data;
  cb_data->requests_info = NULL;
  cb_data->user_context = user_context;
  cb_data->db_handle = db_handle;
  /* Add timer and initialize it. */
  cb_data->timer_id = event_loop_add_timer(db_handle->loop, retry->timeout,
                                           table_timeout_handler, cb_data);
  outstanding_callbacks_add(cb_data);

  cb_data->retry.cb(cb_data);

  return cb_data;
}

void destroy_table_callback(table_callback_data *cb_data) {
  CHECK(cb_data != NULL);

  // if (cb_data->data)
  //   free(cb_data->data);

  if (cb_data->requests_info)
    free(cb_data->requests_info);

  outstanding_callbacks_remove(cb_data);

  /* Timer is removed via EVENT_LOOP_TIMER_DONE in the timeout callback. */
  free(cb_data);
}

int64_t table_timeout_handler(event_loop *loop,
                              int64_t timer_id,
                              void *user_context) {
  CHECK(loop != NULL);
  CHECK(user_context != NULL);
  table_callback_data *cb_data = (table_callback_data *) user_context;

  CHECK(cb_data->retry.count >= 0)
  LOG_DEBUG("retrying operation, retry_count = %d", cb_data->retry.count);

  if (cb_data->retry.count == 0) {
    /* We didn't get a response from the database after exhausting all retries;
     * let user know, cleanup the state, and remove the timer. */
    if (cb_data->fail_cb) {
      cb_data->fail_cb(cb_data->id, cb_data->user_context);
    }
    destroy_table_callback(cb_data);
    return EVENT_LOOP_TIMER_DONE;
  }

  /* Decrement retry count and try again. */
  cb_data->retry.count--;
  cb_data->retry.cb(cb_data);
  return cb_data->retry.timeout;
}

/**
 * List of outstanding callbacks. We need to maintain this list for the case in
 * which a reply is received
 * after te last timeout expires and all relevant data structures are removed.
 * In this case we just need
 * to ignore the reply.
 * */
static outstanding_callback *outstanding_cbs = NULL;

void outstanding_callbacks_add(table_callback_data *key) {
  outstanding_callback *outstanding_cb = malloc(sizeof(outstanding_callback));

  CHECK(outstanding_cb != NULL);
  outstanding_cb->key = key;
  HASH_ADD_PTR(outstanding_cbs, key, outstanding_cb);
}

outstanding_callback *outstanding_callbacks_find(table_callback_data *key) {
  outstanding_callback *outstanding_cb = NULL;

  HASH_FIND_PTR(outstanding_cbs, &key, outstanding_cb);
  return outstanding_cb;
}

void outstanding_callbacks_remove(table_callback_data *key) {
  outstanding_callback *outstanding_cb = NULL;

  outstanding_cb = outstanding_callbacks_find(key);
  if (outstanding_cb != NULL) {
    HASH_DEL(outstanding_cbs, outstanding_cb);
    free(outstanding_cb);
  }
}
