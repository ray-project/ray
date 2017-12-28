#include "object_table.h"
#include "redis.h"

void object_table_lookup(DBHandle *db_handle,
                         ObjectID object_id,
                         RetryInfo *retry,
                         object_table_lookup_done_callback done_callback,
                         void *user_context) {
  CHECK(db_handle != NULL);
  init_table_callback(db_handle, object_id, __func__,
                      new CommonCallbackData(NULL), retry,
                      (table_done_callback) done_callback,
                      redis_object_table_lookup, user_context);
}

void object_table_add(DBHandle *db_handle,
                      ObjectID object_id,
                      int64_t object_size,
                      unsigned char digest[],
                      RetryInfo *retry,
                      object_table_done_callback done_callback,
                      void *user_context) {
  CHECK(db_handle != NULL);

  ObjectTableAddData *info =
      (ObjectTableAddData *) malloc(sizeof(ObjectTableAddData));
  info->object_size = object_size;
  memcpy(&info->digest[0], digest, DIGEST_SIZE);
  init_table_callback(db_handle, object_id, __func__,
                      new CommonCallbackData(info), retry,
                      (table_done_callback) done_callback,
                      redis_object_table_add, user_context);
}

void object_table_remove(DBHandle *db_handle,
                         ObjectID object_id,
                         DBClientID *client_id,
                         RetryInfo *retry,
                         object_table_done_callback done_callback,
                         void *user_context) {
  CHECK(db_handle != NULL);
  /* Copy the client ID, if one was provided. */
  DBClientID *client_id_copy = NULL;
  if (client_id != NULL) {
    client_id_copy = (DBClientID *) malloc(sizeof(DBClientID));
    *client_id_copy = *client_id;
  }
  init_table_callback(db_handle, object_id, __func__,
                      new CommonCallbackData(client_id_copy), retry,
                      (table_done_callback) done_callback,
                      redis_object_table_remove, user_context);
}

void object_table_subscribe_to_notifications(
    DBHandle *db_handle,
    bool subscribe_all,
    object_table_object_available_callback object_available_callback,
    void *subscribe_context,
    RetryInfo *retry,
    object_table_lookup_done_callback done_callback,
    void *user_context) {
  CHECK(db_handle != NULL);
  ObjectTableSubscribeData *sub_data =
      (ObjectTableSubscribeData *) malloc(sizeof(ObjectTableSubscribeData));
  sub_data->object_available_callback = object_available_callback;
  sub_data->subscribe_context = subscribe_context;
  sub_data->subscribe_all = subscribe_all;

  init_table_callback(
      db_handle, ObjectID::nil(), __func__, new CommonCallbackData(sub_data),
      retry, (table_done_callback) done_callback,
      redis_object_table_subscribe_to_notifications, user_context);
}

void object_table_request_notifications(DBHandle *db_handle,
                                        int num_object_ids,
                                        ObjectID object_ids[],
                                        RetryInfo *retry) {
  CHECK(db_handle != NULL);
  CHECK(num_object_ids > 0);
  ObjectTableRequestNotificationsData *data =
      (ObjectTableRequestNotificationsData *) malloc(
          sizeof(ObjectTableRequestNotificationsData) +
          num_object_ids * sizeof(ObjectID));
  data->num_object_ids = num_object_ids;
  memcpy(data->object_ids, object_ids, num_object_ids * sizeof(ObjectID));

  init_table_callback(db_handle, ObjectID::nil(), __func__,
                      new CommonCallbackData(data), retry, NULL,
                      redis_object_table_request_notifications, NULL);
}

void object_info_subscribe(DBHandle *db_handle,
                           object_info_subscribe_callback subscribe_callback,
                           void *subscribe_context,
                           RetryInfo *retry,
                           object_info_done_callback done_callback,
                           void *user_context) {
  ObjectInfoSubscribeData *sub_data =
      (ObjectInfoSubscribeData *) malloc(sizeof(ObjectInfoSubscribeData));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, ObjectID::nil(), __func__,
                      new CommonCallbackData(sub_data), retry,
                      (table_done_callback) done_callback,
                      redis_object_info_subscribe, user_context);
}

void result_table_add(DBHandle *db_handle,
                      ObjectID object_id,
                      TaskID task_id,
                      bool is_put,
                      RetryInfo *retry,
                      result_table_done_callback done_callback,
                      void *user_context) {
  ResultTableAddInfo *info =
      (ResultTableAddInfo *) malloc(sizeof(ResultTableAddInfo));
  info->task_id = task_id;
  info->is_put = is_put;
  init_table_callback(db_handle, object_id, __func__,
                      new CommonCallbackData(info), retry,
                      (table_done_callback) done_callback,
                      redis_result_table_add, user_context);
}

void result_table_lookup(DBHandle *db_handle,
                         ObjectID object_id,
                         RetryInfo *retry,
                         result_table_lookup_callback done_callback,
                         void *user_context) {
  init_table_callback(db_handle, object_id, __func__,
                      new CommonCallbackData(NULL), retry,
                      (table_done_callback) done_callback,
                      redis_result_table_lookup, user_context);
}
