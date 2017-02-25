#include "object_table.h"
#include "redis.h"
#include "object_info.h"

void object_table_lookup(db_handle *db_handle,
                         ObjectID object_id,
                         retry_info *retry,
                         object_table_lookup_done_callback done_callback,
                         void *user_context) {
  CHECK(db_handle != NULL);
  init_table_callback(db_handle, object_id, __func__, NULL, retry,
                      done_callback, redis_object_table_lookup, user_context);
}

void object_table_add(db_handle *db_handle,
                      ObjectID object_id,
                      int64_t object_size,
                      unsigned char digest[],
                      retry_info *retry,
                      object_table_done_callback done_callback,
                      void *user_context) {
  CHECK(db_handle != NULL);

  object_table_add_data *info = malloc(sizeof(object_table_add_data));
  info->object_size = object_size;
  memcpy(&info->digest[0], digest, DIGEST_SIZE);
  init_table_callback(db_handle, object_id, __func__, info, retry,
                      done_callback, redis_object_table_add, user_context);
}

void object_table_remove(db_handle *db_handle,
                         ObjectID object_id,
                         db_client_id *client_id,
                         retry_info *retry,
                         object_table_done_callback done_callback,
                         void *user_context) {
  CHECK(db_handle != NULL);
  /* Copy the client ID, if one was provided. */
  db_client_id *client_id_copy = NULL;
  if (client_id != NULL) {
    client_id_copy = malloc(sizeof(db_client_id));
    *client_id_copy = *client_id;
  }
  init_table_callback(db_handle, object_id, __func__, client_id_copy, retry,
                      done_callback, redis_object_table_remove, user_context);
}

void object_table_subscribe_to_notifications(
    db_handle *db_handle,
    bool subscribe_all,
    object_table_object_available_callback object_available_callback,
    void *subscribe_context,
    retry_info *retry,
    object_table_lookup_done_callback done_callback,
    void *user_context) {
  CHECK(db_handle != NULL);
  object_table_subscribe_data *sub_data =
      malloc(sizeof(object_table_subscribe_data));
  sub_data->object_available_callback = object_available_callback;
  sub_data->subscribe_context = subscribe_context;
  sub_data->subscribe_all = subscribe_all;

  init_table_callback(
      db_handle, NIL_OBJECT_ID, __func__, sub_data, retry, done_callback,
      redis_object_table_subscribe_to_notifications, user_context);
}

void object_table_request_notifications(db_handle *db_handle,
                                        int num_object_ids,
                                        ObjectID object_ids[],
                                        retry_info *retry) {
  CHECK(db_handle != NULL);
  CHECK(num_object_ids > 0);
  object_table_request_notifications_data *data =
      malloc(sizeof(object_table_request_notifications_data) +
             num_object_ids * sizeof(ObjectID));
  data->num_object_ids = num_object_ids;
  memcpy(data->object_ids, object_ids, num_object_ids * sizeof(ObjectID));

  init_table_callback(db_handle, NIL_OBJECT_ID, __func__, data, retry, NULL,
                      redis_object_table_request_notifications, NULL);
}

void object_info_subscribe(db_handle *db_handle,
                           object_info_subscribe_callback subscribe_callback,
                           void *subscribe_context,
                           retry_info *retry,
                           object_info_done_callback done_callback,
                           void *user_context) {
  object_info_subscribe_data *sub_data =
      malloc(sizeof(object_info_subscribe_data));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_OBJECT_ID, __func__, sub_data, retry,
                      done_callback, redis_object_info_subscribe, user_context);
}

void result_table_add(db_handle *db_handle,
                      ObjectID object_id,
                      task_id task_id_arg,
                      retry_info *retry,
                      result_table_done_callback done_callback,
                      void *user_context) {
  task_id *task_id_copy = malloc(sizeof(task_id));
  memcpy(task_id_copy, task_id_arg.id, sizeof(*task_id_copy));
  init_table_callback(db_handle, object_id, __func__, task_id_copy, retry,
                      done_callback, redis_result_table_add, user_context);
}

void result_table_lookup(db_handle *db_handle,
                         ObjectID object_id,
                         retry_info *retry,
                         result_table_lookup_callback done_callback,
                         void *user_context) {
  init_table_callback(db_handle, object_id, __func__, NULL, retry,
                      done_callback, redis_result_table_lookup, user_context);
}
