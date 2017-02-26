#ifndef OBJECT_TABLE_H
#define OBJECT_TABLE_H

#include "common.h"
#include "table.h"
#include "db.h"
#include "task.h"

/*
 *  ==== Lookup call and callback ====
 */

/* Callback called when the lookup completes. The callback should free
 * the manager_vector array, but NOT the strings they are pointing to. If there
 * was no entry at all for the object (the object had never been created
 * before), then manager_count will be -1.
 */
typedef void (*object_table_lookup_done_callback)(
    ObjectID object_id,
    int manager_count,
    OWNER const char *manager_vector[],
    void *user_context);

/* Callback called when object ObjectID is available. */
typedef void (*object_table_object_available_callback)(
    ObjectID object_id,
    int64_t data_size,
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
void object_table_lookup(DBHandle *db_handle,
                         ObjectID object_id,
                         retry_info *retry,
                         object_table_lookup_done_callback done_callback,
                         void *user_context);

/*
 *  ==== Add object call and callback ====
 */

/* Callback called when the object add/remove operation completes. */
typedef void (*object_table_done_callback)(ObjectID object_id,
                                           void *user_context);

/**
 * Add the plasma manager that created the db_handle to the
 * list of plasma managers that have the object_id.
 *
 * @param db_handle Handle to db.
 * @param object_id Object unique identifier.
 * @param data_size Object data size.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Callback to be called when lookup completes.
 * @param user_context User context to be passed in the callbacks.
 * @return Void.
 */
void object_table_add(DBHandle *db_handle,
                      ObjectID object_id,
                      int64_t object_size,
                      unsigned char digest[],
                      retry_info *retry,
                      object_table_done_callback done_callback,
                      void *user_context);

/** Data that is needed to add new objects to the object table. */
typedef struct {
  int64_t object_size;
  unsigned char digest[DIGEST_SIZE];
} object_table_add_data;

/*
 *  ==== Remove object call and callback ====
 */

/**
 * Object remove function.
 *
 * @param db_handle Handle to db.
 * @param object_id Object unique identifier.
 * @param client_id A pointer to the database client ID to remove. If this is
 *        set to NULL, then the client ID associated with db_handle will be
 *        removed.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Callback to be called when lookup completes.
 * @param user_context User context to be passed in the callbacks.
 * @return Void.
 */
void object_table_remove(DBHandle *db_handle,
                         ObjectID object_id,
                         DBClientID *client_id,
                         retry_info *retry,
                         object_table_done_callback done_callback,
                         void *user_context);

/*
 *  ==== Subscribe to be announced when new object available ====
 */

/**
 * Set up a client-specific channel for receiving notifications about available
 * objects from the object table. The callback will be called once per
 * notification received on this channel.
 *
 * @param db_handle Handle to db.
 * @param object_available_callback Callback to be called when new object
 *        becomes available.
 * @param subscribe_context Caller context which will be passed to the
 *        object_available_callback.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Callback to be called when subscription is installed.
 *        This is only used for the tests.
 * @param user_context User context to be passed into the done callback. This is
 *        only used for the tests.
 * @return Void.
 */
void object_table_subscribe_to_notifications(
    DBHandle *db_handle,
    bool subscribe_all,
    object_table_object_available_callback object_available_callback,
    void *subscribe_context,
    retry_info *retry,
    object_table_lookup_done_callback done_callback,
    void *user_context);

/**
 * Request notifications about the availability of some objects from the object
 * table. The notifications will be published to this client's object
 * notification channel, which was set up by the method
 * object_table_subscribe_to_notifications.
 *
 * @param db_handle Handle to db.
 * @param object_ids The object IDs to receive notifications about.
 * @param retry Information about retrying the request to the database.
 * @return Void.
 */
void object_table_request_notifications(DBHandle *db,
                                        int num_object_ids,
                                        ObjectID object_ids[],
                                        retry_info *retry);

/** Data that is needed to run object_request_notifications requests. */
typedef struct {
  /** The number of object IDs. */
  int num_object_ids;
  /** This field is used to store a variable number of object IDs. */
  ObjectID object_ids[0];
} object_table_request_notifications_data;

/** Data that is needed to register new object available callbacks with the
 *  state database. */
typedef struct {
  bool subscribe_all;
  object_table_object_available_callback object_available_callback;
  void *subscribe_context;
} object_table_subscribe_data;

/*
 * ==== Object info table, contains size of the object ====
 */

typedef void (*object_info_done_callback)(ObjectID object_id,
                                          void *user_context);

typedef void (*object_info_subscribe_callback)(ObjectID object_id,
                                               int64_t object_size,
                                               void *user_context);

/**
 * Subcribing to the object info pub/sub channel
 *
 * @param db_handle Handle to db.
 * @param object_info_subscribe_callback callback triggered when pub/sub channel
 *        is notified of a new object size.
 * @param subscribe_context caller context which will be passed back in the
 *        object_info_subscribe_callback.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Callback to be called when subscription is installed.
 * @param user_context User context to be passed into the done and fail
 *        callbacks.
 * @return Void.
 */
void object_info_subscribe(DBHandle *db_handle,
                           object_info_subscribe_callback subscribe_callback,
                           void *subscribe_context,
                           retry_info *retry,
                           object_info_done_callback done_callback,
                           void *user_context);

/* Data that is needed to register new object info callbacks with the state
 * database. */
typedef struct {
  object_info_subscribe_callback subscribe_callback;
  void *subscribe_context;
} object_info_subscribe_data;

/*
 *  ==== Result table ====
 */

/**
 * Callback called when the add/remove operation for a result table entry
 * completes. */
typedef void (*result_table_done_callback)(ObjectID object_id,
                                           void *user_context);

/**
 * Add information about a new object to the object table. This
 * is immutable information like the ID of the task that
 * created the object.
 *
 * @param db_handle Handle to object_table database.
 * @param object_id ID of the object to add.
 * @param task_id ID of the task that creates this object.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Context passed by the caller.
 * @return Void.
 */
void result_table_add(DBHandle *db_handle,
                      ObjectID object_id,
                      TaskID task_id,
                      retry_info *retry,
                      result_table_done_callback done_callback,
                      void *user_context);

/** Callback called when the result table lookup completes. */
typedef void (*result_table_lookup_callback)(ObjectID object_id,
                                             TaskID task_id,
                                             void *user_context);

/**
 * Lookup the task that created an object in the result table. The return value
 * is the task ID.
 *
 * @param db_handle Handle to object_table database.
 * @param object_id ID of the object to lookup.
 * @param retry Information about retrying the request to the database.
 * @param done_callback Function to be called when database returns result.
 * @param user_context Context passed by the caller.
 * @return Void.
 */
void result_table_lookup(DBHandle *db_handle,
                         ObjectID object_id,
                         retry_info *retry,
                         result_table_lookup_callback done_callback,
                         void *user_context);

#endif /* OBJECT_TABLE_H */
