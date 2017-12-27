#include "driver_table.h"
#include "redis.h"

void driver_table_subscribe(DBHandle *db_handle,
                            driver_table_subscribe_callback subscribe_callback,
                            void *subscribe_context,
                            RetryInfo *retry) {
  DriverTableSubscribeData *sub_data =
      (DriverTableSubscribeData *) malloc(sizeof(DriverTableSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;
  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(sub_data), retry, NULL,
                      redis_driver_table_subscribe, NULL);
}

void driver_table_send_driver_death(DBHandle *db_handle,
                                    WorkerID driver_id,
                                    RetryInfo *retry) {
  init_table_callback(db_handle, driver_id, __func__,
                      new CommonCallbackData(NULL), retry, NULL,
                      redis_driver_table_send_driver_death, NULL);
}
