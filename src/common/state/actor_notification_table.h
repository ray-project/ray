#ifndef ACTOR_NOTIFICATION_TABLE_H
#define ACTOR_NOTIFICATION_TABLE_H

#include "task.h"
#include "db.h"
#include "table.h"

/*
 *  ==== Subscribing to the actor notification table ====
 */

/* Callback for subscribing to the local scheduler table. */
typedef void (*actor_notification_table_subscribe_callback)(
    const ActorID &actor_id,
    const WorkerID &driver_id,
    const DBClientID &local_scheduler_id,
    void *user_context);

/// Publish an actor creation notification. This is published by a local
/// scheduler once it creates an actor.
///
/// \param db_handle Database handle.
/// \param actor_id The ID of the actor that was created.
/// \param driver_id The ID of the driver that created the actor.
/// \param local_scheduler_id The ID of the local scheduler that created the
///        actor.
/// \return Void.
void publish_actor_creation_notification(DBHandle *db_handle,
                                         const ActorID &actor_id,
                                         const WorkerID &driver_id,
                                         const DBClientID &local_scheduler_id);

/// Data that is needed to publish an actor creation notification.
typedef struct {
  /// The size of the flatbuffer object.
  int64_t size;
  /// The information to be sent.
  uint8_t flatbuffer_data[0];
} ActorCreationNotificationData;

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
    DBHandle *db_handle,
    actor_notification_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry);

/* Data that is needed to register local scheduler table subscribe callbacks
 * with the state database. */
typedef struct {
  actor_notification_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} ActorNotificationTableSubscribeData;

/**
 * Marks an actor as removed. This prevents the actor from being resurrected.
 *
 * @param db The database handle.
 * @param actor_id The actor id to mark as removed.
 * @return Void.
 */
void actor_table_mark_removed(DBHandle *db_handle, ActorID actor_id);

#endif /* ACTOR_NOTIFICATION_TABLE_H */
