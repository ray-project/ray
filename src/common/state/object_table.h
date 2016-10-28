#ifndef OBJECT_TABLE_H
#define OBJECT_TABLE_H

#include "common.h"
#include "table.h"
#include "db.h"

/*
 *  ==== Lookup call and callback ====
 */

/* Callback called when the lookup completes. The callback should free
 * the manager_vector array, but NOT the strings they are pointing to.
 */
typedef void (*object_table_lookup_done_cb)(object_id object_id,
                                            int manager_count,
                                            OWNER const char *manager_vector[],
                                            void *user_context);

/**
 *  Return the list of nodes storing object_id in their plasma stores.
 *
 *  @param db_handle Handle to object_table database.
 *  @param object_id ID of the object being looked up.
 *  @param retry Information about retrying the request to the database.
 *  @param done_callback Function to be called when database returns result.
 *  @param user_context Context passed by the caller.
 *  @return Void.
 */
void object_table_lookup(db_handle *db_handle,
                         object_id object_id,
                         retry_info *retry,
                         object_table_lookup_done_cb done_cb,
                         void *user_context);

/*
 *  ==== Add object call and callback ====
 */

/* Callback called when the object add/remove operation completes. */
typedef void (*object_table_done_cb)(object_id object_id, void *user_context);

/**
 * Add the plasma manager that created the db_handle to the
 * list of plasma managers that have the object_id.
 *
 * @param db_handle Handle to db.
 * @param object_id Object unique identifier.
 * @param retry Information about retrying the request to the database.
 * @param done_cb Callback to be called when lookup completes.
 * @param user_context User context to be passed in the callbacks.
 * @return Void.
 */
void object_table_add(db_handle *db_handle,
                      object_id object_id,
                      retry_info *retry,
                      object_table_done_cb done_cb,
                      void *user_context);

/*
 *  ==== Remove object call and callback ====
 */

/**
 * Object remove function.
 *
 * @param db_handle Handle to db.
 * @param object_id Object unique identifier.
 * @param retry Information about retrying the request to the database.
 * @param done_cb Callback to be called when lookup completes.
 * @param user_context User context to be passed in the callbacks.
 * @return Void.
 */
/*
void object_table_remove(db_handle *db,
                         object_id object_id,
                         lookup_callback callback,
                         void *context);
                         retry_info *retry,
                         object_table_done_cb done_cb,
                         void *user_context);
*/

/*
 *  ==== Subscribe to be announced when new object available ====
 */

/* Callback called when object object_id is available. */
typedef void (*object_table_object_available_cb)(object_id object_id,
                                                 void *user_context);

/**
 * Subcribing to new object available function.
 *
 * @param db_handle Handle to db.
 * @param object_id Object unique identifier.
 * @param object_available_cb callback to be called when new object becomes
 *        available.
 * @param subscribe_context caller context which will be passed back in the
 *        object_available_cb.
 * @param retry Information about retrying the request to the database.
 * @param done_cb Callback to be called when subscription is installed.
 * @param user_context User context to be passed in the callbacks.
 * @return Void.
 */

void object_table_subscribe(
    db_handle *db,
    object_id object_id,
    object_table_object_available_cb object_available_cb,
    void *subscribe_context,
    retry_info *retry,
    object_table_done_cb done_cb,
    void *user_context);

/* Data that is needed to register new object available callbacks with the state
 * database. */
typedef struct {
  object_table_object_available_cb object_available_cb;
  void *subscribe_context;
} object_table_subscribe_data;

#endif /* OBJECT_TABLE_H */
