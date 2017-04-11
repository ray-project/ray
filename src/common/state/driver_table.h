#ifndef DRIVER_TABLE_H
#define DRIVER_TABLE_H

#include "db.h"
#include "table.h"
#include "task.h"

/*
 *  ==== Subscribing to the driver table ====
 */

/* Callback for subscribing to the driver table. */
typedef void (*driver_table_subscribe_callback)(WorkerID driver_id,
                                                void *user_context);

/**
 * Register a callback for a driver table event.
 *
 * @param db_handle Database handle.
 * @param subscribe_callback Callback that will be called when the driver event
 *        happens.
 * @param subscribe_context Context that will be passed into the
 *        subscribe_callback.
 * @param retry Information about retrying the request to the database.
 * @return Void.
 */
void driver_table_subscribe(
    DBHandle *db_handle,
    driver_table_subscribe_callback subscribe_callback,
    void *subscribe_context,
    RetryInfo *retry);

/* Data that is needed to register driver table subscribe callbacks with the
 * state database. */
typedef struct {
  driver_table_subscribe_callback subscribe_callback;
  void *subscribe_context;
} DriverTableSubscribeData;

/**
 * Send driver death update to all subscribers.
 *
 * @param db_handle Database handle.
 * @param driver_id The ID of the driver that died.
 * @param retry Information about retrying the request to the database.
 */
void driver_table_send_driver_death(DBHandle *db_handle,
                                    WorkerID driver_id,
                                    RetryInfo *retry);

#endif /* DRIVER_TABLE_H */
