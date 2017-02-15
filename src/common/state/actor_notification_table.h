#ifndef ACTOR_NOTIFICATION_TABLE_H
#define ACTOR_NOTIFICATION_TABLE_H

#include "task.h"
#include "db.h"
#include "table.h"

typedef struct {
  /** The ID of the actor. */
  actor_id actor_id;
  /** The ID of the local scheduler that is responsible for the actor. */
  db_client_id local_scheduler_id;
} actor_info;

/*
 *  ==== Subscribing to the actor notification table ====
 */

/* Callback for subscribing to the local scheduler table. */
typedef void (*actor_notification_table_subscribe_callback)(actor_info info,
                                                            void *user_context);

/**
 * Register a callback to process actor notification events.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the local
 *        scheduler event happens.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param retry Information about retrying the request to the database.
 * @return Void.
 */
void actor_notification_table_subscribe(
    db_handle *db_handle,
    actor_notification_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    retry_info *retry);

/* Data that is needed to register local scheduler table subscribe callbacks
 * with the state database. */
typedef struct {
  actor_notification_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} actor_notification_table_subscribe_data;

#endif /* ACTOR_NOTIFICATION_TABLE_H */
