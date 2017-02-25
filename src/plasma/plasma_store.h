#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include "plasma.h"

typedef struct client client;

typedef struct plasma_store_state plasma_store_state;

/**
 * Create a new object. The client must do a call to release_object to tell the
 * store when it is done with the object.
 *
 * @param client_context The context of the client making this request.
 * @param object_id Object ID of the object to be created.
 * @param data_size Size in bytes of the object to be created.
 * @param metadata_size Size in bytes of the object metadata.
 * @return One of the following error codes:
 *         - PlasmaError_OK, if the object was created successfully.
 *         - PlasmaError_ObjectExists, if an object with this ID is already
 *           present in the store. In this case, the client should not call
 *           plasma_release.
 *         - PlasmaError_OutOfMemory, if the store is out of memory and cannot
 *           create the object. In this case, the client should not call
 *           plasma_release.
 */
int create_object(client *client_context,
                  ObjectID object_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  plasma_object *result);

/**
 * Get an object. This method assumes that we currently have or will eventually
 * have this object sealed. If the object has not yet been sealed, the client
 * that requested the object will be notified when it is sealed.
 *
 * For each call to get_object, the client must do a call to release_object to
 * tell the store when it is done with the object.
 *
 * @param client_context The context of the client making this request.
 * @param conn The client connection that requests the object.
 * @param object_id Object ID of the object to be gotten.
 * @return The status of the object (object_status in plasma.h).
 */
int get_object(client *client_context,
               int conn,
               ObjectID object_id,
               plasma_object *result);

/**
 * Get an object from the local Plasma Store. This function is not blocking.
 *
 * Once a client gets an object it must release it when it is done with it.
 * This function is indepontent. If a client calls repeatedly get_object_local()
 * on the same object_id, the client needs to call release_object() only once.
 *
 * @param client_context The context of the client making this request.
 * @param conn The client connection that requests the object.
 * @param object_id Object ID of the object to be gotten.
 * @return Return OBJECT_FOUND if object was found, and OBJECT_NOT_FOUND
 *         otherwise.
 */
int get_object_local(client *client_context,
                     int conn,
                     ObjectID object_id,
                     plasma_object *result);

/**
 * Record the fact that a particular client is no longer using an object.
 *
 * @param client_context The context of the client making this request.
 * @param object_id The object ID of the object that is being released.
 * @param Void.
 */
void release_object(client *client_context, ObjectID object_id);

/**
 * Seal an object. The object is now immutable and can be accessed with get.
 *
 * @param client_context The context of the client making this request.
 * @param object_id Object ID of the object to be sealed.
 * @param digest The digest of the object. This is used to tell if two objects
 *        with the same object ID are the same.
 * @return Void.
 */
void seal_object(client *client_context,
                 ObjectID object_id,
                 unsigned char digest[]);

/**
 * Check if the plasma store contains an object:
 *
 * @param client_context The context of the client making this request.
 * @param object_id Object ID that will be checked.
 * @return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if not
 */
int contains_object(client *client_context, ObjectID object_id);

/**
 * Send notifications about sealed objects to the subscribers. This is called
 * in seal_object. If the socket's send buffer is full, the notification will be
 * buffered, and this will be called again when the send buffer has room.
 *
 * @param loop The Plasma store event loop.
 * @param client_sock The socket of the client to send the notification to.
 * @param plasma_state The plasma store global state.
 * @param events This is needed for this function to have the signature of a
          callback.
 * @return Void.
 */
void send_notifications(event_loop *loop,
                        int client_sock,
                        void *plasma_state,
                        int events);

void remove_objects(plasma_store_state *plasma_state,
                    int64_t num_objects_to_evict,
                    ObjectID *objects_to_evict);

#endif /* PLASMA_STORE_H */
