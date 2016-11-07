#include "object_table.h"
#include "redis.h"

void object_table_lookup_location(
    db_handle *db_handle,
    object_id object_id,
    retry_info *retry,
    object_table_lookup_location_done_callback done_callback,
    void *user_context) {
  init_table_callback(db_handle, object_id, NULL, retry, done_callback,
                      redis_object_table_lookup_location, user_context);
}

void object_table_add_location(
    db_handle *db_handle,
    object_id object_id,
    retry_info *retry,
    object_table_location_done_callback done_callback,
    void *user_context) {
  init_table_callback(db_handle, object_id, NULL, retry, done_callback,
                      redis_object_table_add_location, user_context);
}

void object_table_subscribe(
    db_handle *db_handle,
    object_id object_id,
    object_table_object_available_callback object_available_callback,
    void *subscribe_context,
    retry_info *retry,
    object_table_location_done_callback done_callback,
    void *user_context) {
  object_table_subscribe_data *sub_data =
      malloc(sizeof(object_table_subscribe_data));
  utarray_push_back(db_handle->callback_freelist, &sub_data);
  sub_data->object_available_callback = object_available_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, object_id, sub_data, retry, done_callback,
                      redis_object_table_subscribe, user_context);
}

void free_object_metadata(object_metadata *metadata) {
  if (metadata->task) {
    free_task_spec(metadata->task);
  }
  free(metadata);
}

void object_table_new_object(db_handle *db_handle,
                             object_id object_id,
                             task_id task_id,
                             retry_info *retry,
                             object_table_metadata_done_callback done_callback,
                             void *user_context) {
  object_metadata *metadata = malloc(sizeof(object_metadata));
  metadata->task_id = task_id;
  metadata->task = NULL;
  init_table_callback(db_handle, object_id, metadata, retry, done_callback,
                      redis_object_table_new_object, user_context);
}

void object_table_lookup_metadata(
    db_handle *db_handle,
    object_id object_id,
    retry_info *retry,
    object_table_metadata_done_callback done_callback,
    void *user_context) {
  /* Allocate a blank result for the callbacks to fill in. */
  object_metadata *metadata_result = malloc(sizeof(object_metadata));
  memset(metadata_result, 0, sizeof(object_metadata));
  init_table_callback(db_handle, object_id, metadata_result, retry,
                      done_callback, redis_object_table_lookup_metadata,
                      user_context);
}
