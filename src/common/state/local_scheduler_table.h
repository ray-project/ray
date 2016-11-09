#ifndef local_scheduler_table_H
#define local_scheduler_table_H

#include "db.h"
#include "table.h"

typedef void (*local_scheduler_table_done_callback)(client_id client_id,
                                                    void *user_context);

/*
 *  ==== Subscribing to the local scheduler table ====
 */

/* Callback for subscribing to the local scheduler table. TODO(rkn): more info
 * could be passed in besides the client ID. For example, it could include
 * information about the local scheduler utilization. */
typedef void (*local_scheduler_table_subscribe_callback)(client_id client_id,
                                                         void *user_context);

/**
 * Register a callback for a local scheduler event. An event is any update of a
 * local scheduler in the local scheduler table.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the local
 *        scheduler table is updated.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Data that will be passed to done_callback and
 *        fail_callback.
 * @return Void.
 */
void local_scheduler_table_subscribe(
    db_handle *db_handle,
    local_scheduler_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry,
    local_scheduler_table_done_callback done_callback,
    void *user_context);

/* Data that is needed to register local scheduler table subscribe callbacks
 * with the state database. */
typedef struct {
  local_scheduler_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} local_scheduler_table_subscribe_data;

#endif /* local_scheduler_table_H */
