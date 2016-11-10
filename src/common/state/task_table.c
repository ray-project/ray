#include "task_table.h"
#include "redis.h"

#define NUM_DB_REQUESTS 2

void task_table_get_task(db_handle *db_handle,
                         task_id task_id,
                         retry_info *retry,
                         task_table_get_callback done_callback,
                         void *user_context) {
  init_table_callback(db_handle, task_id, NULL, retry, done_callback,
                      redis_task_table_get_task, user_context);
}

void task_table_add_task(db_handle *db_handle,
                         task *task,
                         retry_info *retry,
                         task_table_done_callback done_callback,
                         void *user_context) {
  init_table_callback(db_handle, task_task_id(task), task, retry, done_callback,
                      redis_task_table_add_task, user_context);
}

void task_table_update(db_handle *db_handle,
                       task *task,
                       retry_info *retry,
                       task_table_done_callback done_callback,
                       void *user_context) {
  init_table_callback(db_handle, task_task_id(task), task, retry, done_callback,
                      redis_task_table_update, user_context);
}

/* TODO(swang): A corresponding task_table_unsubscribe. */
void task_table_subscribe(db_handle *db_handle,
                          node_id node,
                          scheduling_state state_filter,
                          task_table_subscribe_callback subscribe_callback,
                          void *subscribe_context,
                          retry_info *retry,
                          task_table_done_callback done_callback,
                          void *user_context) {
  task_table_subscribe_data *sub_data =
      malloc(sizeof(task_table_subscribe_data));
  utarray_push_back(db_handle->callback_freelist, &sub_data);
  sub_data->node = node;
  sub_data->state_filter = state_filter;
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, node, sub_data, retry, done_callback,
                      redis_task_table_subscribe, user_context);
}
