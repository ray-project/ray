#include "db_client_table.h"
#include "redis.h"

void db_client_table_subscribe(
    db_handle *db_handle,
    db_client_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry,
    db_client_table_done_callback done_callback,
    void *user_context) {
  db_client_table_subscribe_data *sub_data =
      malloc(sizeof(db_client_table_subscribe_data));
  sub_data->subscribe_callback = subscribe_callback;
  sub_data->subscribe_context = subscribe_context;

  init_table_callback(db_handle, NIL_ID, __func__, sub_data, retry,
                      done_callback, redis_db_client_table_subscribe,
                      user_context);
}
