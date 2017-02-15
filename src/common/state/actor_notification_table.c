#include "actor_notification_table.h"
#include "redis.h"

void actor_notification_table_subscribe(
    db_handle *db_handle,
    actor_notification_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry) {
  actor_notification_table_subscribe_data *sub_data =
      malloc(sizeof(actor_notification_table_subscribe_data));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry, NULL,
                      redis_actor_notification_table_subscribe, NULL);
}
