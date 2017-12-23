#include "common_protocol.h"
#include "local_scheduler_table.h"
#include "redis.h"

void local_scheduler_table_subscribe(
    DBHandle *db_handle,
    local_scheduler_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry) {
  LocalSchedulerTableSubscribeData *sub_data =
      (LocalSchedulerTableSubscribeData *) malloc(
          sizeof(LocalSchedulerTableSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(sub_data), retry, NULL,
                      redis_local_scheduler_table_subscribe, NULL);
}

void local_scheduler_table_send_info(DBHandle *db_handle,
                                     LocalSchedulerInfo *info,
                                     RetryInfo *retry) {
  /* Create a flatbuffer object to serialize and publish. */
  flatbuffers::FlatBufferBuilder fbb;
  /* Create the flatbuffers message. */
  auto message = CreateLocalSchedulerInfoMessage(
      fbb, to_flatbuf(fbb, db_handle->client), info->total_num_workers,
      info->task_queue_length, info->available_workers,
      map_to_flatbuf(fbb, info->static_resources),
      map_to_flatbuf(fbb, info->dynamic_resources), false);
  fbb.Finish(message);

  LocalSchedulerTableSendInfoData *data =
      (LocalSchedulerTableSendInfoData *) malloc(
          sizeof(LocalSchedulerTableSendInfoData) + fbb.GetSize());
  data->size = fbb.GetSize();
  memcpy(&data->flatbuffer_data[0], fbb.GetBufferPointer(), fbb.GetSize());

  init_table_callback(db_handle, UniqueID::nil(), __func__,
                      new CommonCallbackData(data), retry, NULL,
                      redis_local_scheduler_table_send_info, NULL);
}

void local_scheduler_table_disconnect(DBHandle *db_handle) {
  redis_local_scheduler_table_disconnect(db_handle);
}
