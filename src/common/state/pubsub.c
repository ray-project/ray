#include "pubsub.h"
#include "redis.h"

void pubsub_request_transfer(db_handle *db,
                             object_id object_id,
                             int source,
                             int destination,
                             retry_info *retry,
                             pubsub_transfer_callback done_callback,
                             void *user_context) {
  transfer_data *data = malloc(sizeof(transfer_data));
  data->object_id = object_id;
  data->source = source;
  data->destination = destination;
  data->subscribe_callback = NULL;
  init_table_callback(db, object_id, __func__, data, retry, done_callback,
                      redis_pubsub_request_transfer, user_context);
}

void pubsub_subscribe_transfer(db_handle *db,
                               int source,
                               pubsub_subscribe_transfer_callback subscribe_callback,
                               retry_info *retry,
                               pubsub_transfer_callback done_callback,
                               void *user_context) {
  transfer_data *data = malloc(sizeof(transfer_data));
  data->object_id = NIL_OBJECT_ID;
  data->source = source;
  data->destination = -1;
  data->subscribe_callback = subscribe_callback;
  init_table_callback(db, NIL_OBJECT_ID, __func__, data, retry, done_callback,
                      redis_pubsub_subscribe_transfer, user_context);
}
