#include "actor_notification_table.h"

#include "common_protocol.h"
#include "redis.h"

void publish_actor_creation_notification(DBHandle *db_handle,
                                         const ActorID &actor_id,
                                         const WorkerID &driver_id,
                                         const DBClientID &local_scheduler_id) {
  // Create a flatbuffer object to serialize and publish.
  flatbuffers::FlatBufferBuilder fbb;
  // Create the flatbuffers message.
  auto message = CreateActorCreationNotification(
      fbb, to_flatbuf(fbb, actor_id), to_flatbuf(fbb, driver_id),
      to_flatbuf(fbb, local_scheduler_id));
  fbb.Finish(message);

  ActorCreationNotificationData *data =
      (ActorCreationNotificationData *) malloc(
          sizeof(ActorCreationNotificationData) + fbb.GetSize());
  data->size = fbb.GetSize();
  memcpy(&data->flatbuffer_data[0], fbb.GetBufferPointer(), fbb.GetSize());

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(data), NULL, NULL,
                      redis_publish_actor_creation_notification, NULL);
}

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
