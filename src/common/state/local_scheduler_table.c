#include "local_scheduler_table.h"
#include "redis.h"

void local_scheduler_table_subscribe(
    db_handle *db_handle,
    local_scheduler_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry,
    local_scheduler_table_done_callback done_callback,
    void *user_context) {
  local_scheduler_table_subscribe_data *sub_data =
      malloc(sizeof(local_scheduler_table_subscribe_data));
  utarray_push_back(db_handle->callback_freelist, &sub_data);
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry,
                      done_callback, redis_local_scheduler_table_subscribe,
                      user_context);
}
