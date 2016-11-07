#include "task_table.h"
#include "redis.h"

void task_table_add_task(db_handle *db,
                         task_id task_id,
                         task_spec *task,
                         retry_info *retry,
                         task_table_callback done_callback,
                         void *user_context) {
  init_table_callback(db, task_id, task, retry, done_callback,
      redis_task_table_add_task, user_context);
}

void task_table_get_task(db_handle *db,
                         task_id task_id,
                         retry_info *retry,
                         task_table_callback done_callback,
                         void *user_context) {
  init_table_callback(db, task_id, NULL, retry, done_callback,
      redis_task_table_get_task, user_context);
}
