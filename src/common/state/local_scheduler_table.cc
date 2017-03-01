#include "local_scheduler_table.h"
#include "redis.h"

void local_scheduler_table_subscribe(
    DBHandle *db_handle,
    local_scheduler_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry) {
  LocalSchedulerTableSubscribeData *sub_data =
      (LocalSchedulerTableSubscribeData *) malloc(sizeof(LocalSchedulerTableSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry, NULL,
                      redis_local_scheduler_table_subscribe, NULL);
}

void local_scheduler_table_send_info(DBHandle *db_handle,
                                     LocalSchedulerInfo *info,
                                     RetryInfo *retry) {
  LocalSchedulerTableSendInfoData *data =
      (LocalSchedulerTableSendInfoData *) malloc(sizeof(LocalSchedulerTableSendInfoData));
  data->info = *info;

  init_table_callback(db_handle, NIL_ID, __func__, data, retry, NULL,
                      redis_local_scheduler_table_send_info, NULL);
}
