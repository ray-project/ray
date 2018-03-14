#include "table.h"

#include <unordered_map>
#include <inttypes.h>
#include "redis.h"

BaseCallbackData::BaseCallbackData(void *data) {
  data_ = data;
}

BaseCallbackData::~BaseCallbackData(void) {}

void *BaseCallbackData::Get(void) {
  return data_;
}

CommonCallbackData::CommonCallbackData(void *data) : BaseCallbackData(data) {}

CommonCallbackData::~CommonCallbackData(void) {
  free(data_);
}

TaskCallbackData::TaskCallbackData(Task *task_data)
    : BaseCallbackData(task_data) {}

TaskCallbackData::~TaskCallbackData(void) {
  Task *task = (Task *) data_;
  Task_free(task);
}

/* The default behavior is to retry every ten seconds forever. */
static const RetryInfo default_retry = {.num_retries = -1,
                                        .timeout = 10000,
                                        .fail_callback = NULL};

static int64_t callback_data_id = 0;

TableCallbackData *init_table_callback(DBHandle *db_handle,
                                       UniqueID id,
                                       const char *label,
                                       OWNER BaseCallbackData *data,
                                       RetryInfo *retry,
                                       table_done_callback done_callback,
                                       table_retry_callback retry_callback,
                                       void *user_context) {
  RAY_CHECK(db_handle);
  RAY_CHECK(db_handle->loop);
  RAY_CHECK(data);
  /* If no retry info is provided, use the default retry info. */
  if (retry == NULL) {
    retry = (RetryInfo *) &default_retry;
  }
  RAY_CHECK(retry);
  /* Allocate and initialize callback data structure for object table */
  TableCallbackData *callback_data =
      (TableCallbackData *) malloc(sizeof(TableCallbackData));
  RAY_CHECK(callback_data != NULL) << "Memory allocation error!";
  callback_data->id = id;
  callback_data->label = label;
  callback_data->retry = *retry;
  callback_data->done_callback = done_callback;
  callback_data->retry_callback = retry_callback;
  callback_data->data = data;
  callback_data->requests_info = NULL;
  callback_data->user_context = user_context;
  callback_data->db_handle = db_handle;
  /* TODO(ekl) set a retry timer once we've figured out the retry conditions
   * and have a solution to the O(n^2) ae timers issue. For now, use a dummy
   * timer id to uniquely id this callback. */
  callback_data->timer_id = callback_data_id++;
  outstanding_callbacks_add(callback_data);

  RAY_LOG(DEBUG) << "Initializing table command " << callback_data->label
                 << " with timer ID " << callback_data->timer_id;
  callback_data->retry_callback(callback_data);

  return callback_data;
}

void destroy_timer_callback(event_loop *loop,
                            TableCallbackData *callback_data) {
  /* This is commented out because we no longer add timers to the event loop for
   * each Redis command. */
  // event_loop_remove_timer(loop, callback_data->timer_id);
  destroy_table_callback(callback_data);
}

void remove_timer_callback(event_loop *loop, TableCallbackData *callback_data) {
  /* This is commented out because we no longer add timers to the event loop for
   * each Redis command. */
  // event_loop_remove_timer(loop, callback_data->timer_id);
}

void destroy_table_callback(TableCallbackData *callback_data) {
  RAY_CHECK(callback_data != NULL);

  if (callback_data->requests_info)
    free(callback_data->requests_info);

  RAY_CHECK(callback_data->data != NULL);
  delete callback_data->data;
  callback_data->data = NULL;

  outstanding_callbacks_remove(callback_data);

  /* Timer is removed via EVENT_LOOP_TIMER_DONE in the timeout callback. */
  free(callback_data);
}

int64_t table_timeout_handler(event_loop *loop,
                              int64_t timer_id,
                              void *user_context) {
  RAY_CHECK(loop != NULL);
  RAY_CHECK(user_context != NULL);
  TableCallbackData *callback_data = (TableCallbackData *) user_context;

  RAY_CHECK(callback_data->retry.num_retries >= 0 ||
            callback_data->retry.num_retries == -1);
  RAY_LOG(WARNING) << "retrying operation " << callback_data->label
                   << ", retry_count = " << callback_data->retry.num_retries;

  if (callback_data->retry.num_retries == 0) {
    /* We didn't get a response from the database after exhausting all retries;
     * let user know, cleanup the state, and remove the timer. */
    RAY_LOG(WARNING) << "Table command " << callback_data->label
                     << " with timer ID " << timer_id << " failed";
    if (callback_data->retry.fail_callback) {
      callback_data->retry.fail_callback(callback_data->id,
                                         callback_data->user_context,
                                         callback_data->data->Get());
    }
    destroy_table_callback(callback_data);
    return EVENT_LOOP_TIMER_DONE;
  }

  /* Decrement retry count and try again. We use -1 to indicate infinite
   * retries. */
  if (callback_data->retry.num_retries != -1) {
    callback_data->retry.num_retries--;
  }
  callback_data->retry_callback(callback_data);
  return callback_data->retry.timeout;
}

/**
 * Unordered map maintaining the outstanding callbacks.
 *
 * This unordered map is used to handle the following case:
 * - a table command is issued with an associated callback and a callback data
 *   structure;
 * - the last timeout associated to this command expires, as a result the
 *   callback data structure is freed;
 * - a reply arrives, but now the callback data structure is gone, so we have
 *   to ignore this reply;
 *
 * This unordered map enables us to ignore such replies. The operations on the
 * unordered map are as follows.
 *
 * When we issue a table command and a timeout event to wait for the reply, we
 * add a new entry to the unordered map that is keyed by the ID of the timer.
 * Note that table commands must have unique timer IDs, which are assigned by
 * the Redis ae event loop.
 *
 * When we receive the reply, we check whether the callback still exists in
 * this unordered map, and if not we just ignore the reply. If the callback does
 * exist, the reply receiver is responsible for removing the timer and the
 * entry associated to the callback, or else the timeout handler will continue
 * firing.
 *
 * When the last timeout associated to the command expires we remove the entry
 * associated to the callback.
 */
static std::unordered_map<timer_id, TableCallbackData *> outstanding_callbacks;

void outstanding_callbacks_add(TableCallbackData *callback_data) {
  outstanding_callbacks[callback_data->timer_id] = callback_data;
}

TableCallbackData *outstanding_callbacks_find(int64_t key) {
  auto it = outstanding_callbacks.find(key);
  if (it != outstanding_callbacks.end()) {
    return it->second;
  }
  return NULL;
}

void outstanding_callbacks_remove(TableCallbackData *callback_data) {
  outstanding_callbacks.erase(callback_data->timer_id);
}

void destroy_outstanding_callbacks(event_loop *loop) {
  /* We have to be careful because destroy_timer_callback modifies
   * outstanding_callbacks in place */
  auto it = outstanding_callbacks.begin();
  while (it != outstanding_callbacks.end()) {
    auto next_it = std::next(it, 1);
    destroy_timer_callback(loop, it->second);
    it = next_it;
  }
}
