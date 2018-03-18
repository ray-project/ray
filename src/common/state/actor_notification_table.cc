#include "actor_notification_table.h"
#include "redis.h"

void actor_notification_table_subscribe(
    DBHandle *db_handle,
    actor_notification_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry) {
  ActorNotificationTableSubscribeData *sub_data =
      (ActorNotificationTableSubscribeData *) malloc(
          sizeof(ActorNotificationTableSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(sub_data), retry, NULL,
                      redis_actor_notification_table_subscribe, NULL);
}

void actor_table_mark_removed(DBHandle *db_handle, ActorID actor_id) {
  redis_actor_table_mark_removed(db_handle, actor_id);
}
