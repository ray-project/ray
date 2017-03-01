#include "task_table.h"
#include "redis.h"

#define NUM_DB_REQUESTS 2

void task_table_get_task(DBHandle *db_handle,
                         TaskID task_id,
                         RetryInfo *retry,
                         task_table_get_callback get_callback,
                         void *user_context) {
  init_table_callback(db_handle, task_id, __func__, NULL, retry,
                      (void *) get_callback, redis_task_table_get_task, user_context);
}

void task_table_add_task(DBHandle *db_handle,
                         OWNER Task *task,
                         RetryInfo *retry,
                         task_table_done_callback done_callback,
                         void *user_context) {
  init_table_callback(db_handle, Task_task_id(task), __func__, task, retry,
                      (table_done_callback) done_callback, redis_task_table_add_task, user_context);
}

void task_table_update(DBHandle *db_handle,
                       OWNER Task *task,
                       RetryInfo *retry,
                       task_table_done_callback done_callback,
                       void *user_context) {
  init_table_callback(db_handle, Task_task_id(task), __func__, task, retry,
                      (table_done_callback) done_callback, redis_task_table_update, user_context);
}

void task_table_test_and_update(DBHandle *db_handle,
                                TaskID task_id,
                                int test_state_bitmask,
                                int update_state,
                                RetryInfo *retry,
                                task_table_get_callback done_callback,
                                void *user_context) {
  TaskTableTestAndUpdateData *update_data =
      (TaskTableTestAndUpdateData *) malloc(sizeof(TaskTableTestAndUpdateData));
  update_data->test_state_bitmask = test_state_bitmask;
  update_data->update_state = update_state;
  /* Update the task entry's local scheduler with this client's ID. */
  update_data->local_scheduler_id = db_handle->client;
  init_table_callback(db_handle, task_id, __func__, update_data, retry,
                      (table_done_callback) done_callback, redis_task_table_test_and_update,
                      user_context);
}

/* TODO(swang): A corresponding task_table_unsubscribe. */
void task_table_subscribe(DBHandle *db_handle,
                          DBClientID local_scheduler_id,
                          int state_filter,
                          task_table_subscribe_callback subscribe_callback,
                          void *subscribe_context,
                          RetryInfo *retry,
                          task_table_done_callback done_callback,
                          void *user_context) {
  TaskTableSubscribeData *sub_data = (TaskTableSubscribeData *) malloc(sizeof(TaskTableSubscribeData));
  sub_data->local_scheduler_id = local_scheduler_id;
  sub_data->state_filter = state_filter;
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, local_scheduler_id, __func__, sub_data, retry,
                      (table_done_callback) done_callback, redis_task_table_subscribe, user_context);
}
