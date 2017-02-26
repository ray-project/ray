#include "local_scheduler_table.h"
#include "redis.h"

void local_scheduler_table_subscribe(
    DBHandle *db_handle,
    local_scheduler_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry) {
  local_scheduler_table_subscribe_data *sub_data =
      malloc(sizeof(local_scheduler_table_subscribe_data));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry, NULL,
                      redis_local_scheduler_table_subscribe, NULL);
}

void local_scheduler_table_send_info(DBHandle *db_handle,
                                     local_scheduler_info *info,
                                     retry_info *retry) {
  local_scheduler_table_send_info_data *data =
      malloc(sizeof(local_scheduler_table_send_info_data));
  data->info = *info;

  init_table_callback(db_handle, NIL_ID, __func__, data, retry, NULL,
                      redis_local_scheduler_table_send_info, NULL);
}
