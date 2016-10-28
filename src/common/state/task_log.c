#include "task_log.h"
#include "redis.h"

#define NUM_DB_REQUESTS 2

void task_log_publish(db_handle *db_handle,
                      task_instance *task_instance,
                      retry_info *retry,
                      task_log_done_cb done_cb,
                      void *user_context) {
  init_table_callback(db_handle, *task_instance_id(task_instance),
                      task_instance, retry, done_cb, redis_task_log_publish,
                      user_context);
}

void task_log_subscribe(db_handle *db_handle,
                        node_id node,
                        int32_t state_filter,
                        task_log_subscribe_cb subscribe_cb,
                        void *subscribe_context,
                        retry_info *retry,
                        task_log_done_cb done_cb,
                        void *user_context) {
  task_log_subscribe_data *sub_data = malloc(sizeof(task_log_subscribe_data));
  utarray_push_back(db_handle->callback_freelist, &sub_data);
  sub_data->node = node;
  sub_data->state_filter = state_filter;
  sub_data->subscribe_cb = subscribe_cb;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, node, sub_data, retry, done_cb,
                      redis_task_log_subscribe, user_context);
}
