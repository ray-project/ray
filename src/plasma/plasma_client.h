#ifndef PLASMA_CLIENT_H
#define PLASMA_CLIENT_H

#include <stdbool.h>
#include <time.h>

#include "plasma.h"

#define PLASMA_DEFAULT_RELEASE_DELAY 64

typedef struct plasma_connection plasma_connection;

/**
 * Try to connect to the socket several times. If unsuccessful, fail.
 *
 * @param socket_name Name of the Unix domain socket to connect to.
 * @param num_retries Number of retries.
 * @param timeout Timeout in milliseconds.
 * @return File descriptor of the socket.
 */
int socket_connect_retry(const char *socket_name,
                         int num_retries,
                         int64_t timeout);

/**
 * Connect to the local plasma store and plasma manager. Return
 * the resulting connection.
 *
 * @param store_socket_name The name of the UNIX domain socket to use to
 *        connect to the Plasma store.
 * @param manager_socket_name The name of the UNIX domain socket to use to
 *        connect to the local Plasma manager. If this is NULL, then this
 *        function will not connect to a manager.
 * @return The object containing the connection state.
 */
plasma_connection *plasma_connect(const char *store_socket_name,
                                  const char *manager_socket_name,
                                  int release_delay);

/**
 * Disconnect from the local plasma instance, including the local store and
 * manager.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return Void.
 */
void plasma_disconnect(plasma_connection *conn);

/**
 * Return true if the plasma manager is connected.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return True if the plasma manager is connected and false otherwise.
 */
bool plasma_manager_is_connected(plasma_connection *conn);

/**
 * Try to connect to a possibly remote Plasma Manager.
 *
 * @param addr The IP address of the Plasma Manager to connect to.
 * @param port The port of the Plasma Manager to connect to.
 * @return The file descriptor to use to send messages to the
 *         Plasma Manager. If connection was unsuccessful, this
 *         value is -1.
 */
int plasma_manager_connect(const char *addr, int port);

/**
 * Create an object in the Plasma Store. Any metadata for this object must be
 * be passed in when the object is created.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID to use for the newly created object.
 * @param size The size in bytes of the space to be allocated for this object's
          data (this does not include space used for metadata).
 * @param metadata The object's metadata. If there is no metadata, this pointer
          should be NULL.
 * @param metadata_size The size in bytes of the metadata. If there is no
          metadata, this should be 0.
 * @param data The address of the newly created object will be written here.
 * @return True, if object was created, false, otherwise (e.g., if object has
 *         been already created).
 */
bool plasma_create(plasma_connection *conn,
                   object_id object_id,
                   int64_t size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data);

/**
 * Get an object from the Plasma Store. This function will block until the
 * object has been created and sealed in the Plasma Store.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to get.
 * @param size The size in bytes of the retrieved object will be written at this
          address.
 * @param data The address of the object will be written at this address.
 * @param metadata_size The size in bytes of the object's metadata will be
 *        written at this address.
 * @param metadata The address of the object's metadata will be written at this
 *        address.
 * @return Void.
 */
void plasma_get(plasma_connection *conn,
                object_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata);

/**
 * Tell Plasma that the client no longer needs the object. This should be called
 * after plasma_get when the client is done with the object. After this call,
 * the address returned by plasma_get is no longer valid. This should be called
 * once for each call to plasma_get (with the same object ID).
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object that is no longer needed.
 * @return Void.
 */
void plasma_release(plasma_connection *conn, object_id object_id);

/**
 * Check if the object store contains a particular object and the object has
 * been sealed. The result will be stored in has_object.
 *
 * @todo: We may want to indicate if the object has been created but not sealed.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose presence we are checking.
 * @param has_object The function will write 1 at this address if the object is
 *        present and 0 if it is not present.
 * @return Void.
 */
void plasma_contains(plasma_connection *conn,
                     object_id object_id,
                     int *has_object);

/**
 * Seal an object in the object store. The object will be immutable after this
 * call.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to seal.
 * @return Void.
 */
void plasma_seal(plasma_connection *conn, object_id object_id);

/**
 * Delete an object from the object store. This currently assumes that the
 * object is present and has been sealed.
 *
 * @todo We may want to allow the deletion of objects that are not present or
 *       haven't been sealed.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to delete.
 * @return Void.
 */
void plasma_delete(plasma_connection *conn, object_id object_id);

/**
 * Delete objects until we have freed up num_bytes bytes or there are no more
 * released objects that can be deleted.
 *
 * @param conn The object containing the connection state.
 * @param num_bytes The number of bytes to try to free up.
 * @return The total number of bytes of space retrieved.
 */
int64_t plasma_evict(plasma_connection *conn, int64_t num_bytes);

/**
 * Fetch objects from remote plasma stores that have the
 * objects stored.
 *
 * @param manager A file descriptor for the socket connection
 *        to the local manager.
 * @param object_id_count The number of object IDs requested.
 * @param object_ids[] The vector of object IDs requested. Length must be at
 *        least num_object_ids.
 * @param is_fetched[] The vector in which to return the success
 *        of each object's fetch operation, in the same order as
 *        object_ids. Length must be at least num_object_ids.
 * @return Void.
 */
void plasma_fetch(plasma_connection *conn,
                  int num_object_ids,
                  object_id object_ids[],
                  int is_fetched[]);

/**
 * Transfer local object to a different plasma manager.
 *
 * @param conn The object containing the connection state.
 * @param addr IP address of the plasma manager we are transfering to.
 * @param port Port of the plasma manager we are transfering to.
 * @object_id ObjectID of the object we are transfering.
 *
 * @return Void.
 */
void plasma_transfer(plasma_connection *conn,
                     const char *addr,
                     int port,
                     object_id object_id);

/**
 * Wait for objects to be created (right now, wait for local objects).
 *
 * @param conn The object containing the connection state.
 * @param num_object_ids Number of object IDs wait is called on.
 * @param object_ids Object IDs wait is called on.
 * @param timeout Wait will time out and return after this number of ms.
 * @param num_returns Number of object IDs wait will return if it doesn't time
 *        out.
 * @param return_object_ids Out parameter for the object IDs returned by wait.
 *        This is an array of size num_returns. If the number of objects that
 *        are ready when we time out, the objects will be stored in the last
 *        slots of the array and the number of objects is returned.
 * @return Number of objects that are actually ready.
 */
int plasma_wait(plasma_connection *conn,
                int num_object_ids,
                object_id object_ids[],
                uint64_t timeout,
                int num_returns,
                object_id return_object_ids[]);

/**
 * Subscribe to notifications when objects are sealed in the object store.
 * Whenever an object is sealed, a message will be written to the client socket
 * that is returned by this method.
 *
 * @param conn The object containing the connection state.
 * @return The file descriptor that the client should use to read notifications
           from the object store about sealed objects.
 */
int plasma_subscribe(plasma_connection *conn);

/**
 * Get the file descriptor for the socket connection to the plasma manager.
 *
 * @param conn The plasma connection.
 * @return The file descriptor for the manager connection. If there is no
 *         connection to the manager, this is -1.
 */
int get_manager_fd(plasma_connection *conn);

/* === ALTERNATE PLASMA CLIENT API === */

/**
 * Object buffer data structure.
 */
typedef struct {
  /** The size in bytes of the data object. */
  int64_t data_size;
  /** The address of the data object. */
  uint8_t *data;
  /** The metadata size in bytes. */
  int64_t metadata_size;
  /** The address of the metadata. */
  uint8_t *metadata;
} object_buffer;

/**
 * Object information data structure.
 */
typedef struct {
  /** The time when the object was created (sealed). */
  time_t last_access_time;
  /** The time when the object was last accessed. */
  time_t creation_date;
  uint64_t refcount;
} object_info;

/**
 * Get specified object from the local Plasma Store. This function is
 * non-blocking.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to get.
 * @param object_buffer The data structure where the object information will
 *        be written, including object payload and metadata.
 * @return True if the object is returned and false otherwise.
 */
bool plasma_get_local(plasma_connection *conn,
                      object_id object_id,
                      object_buffer *object_buffer);

/**
 * Initiates the fetch (transfer) of an object from a remote Plasma Store.
 *
 * If the object is stored in the local Plasma Store, tell the caller.
 *
 * If not, check whether the object is stored on a remote Plasma Store. If yes,
 * and if a transfer for the object has either been scheduled or is in progress,
 * then return. Otherwise schedule a transfer for the object.
 *
 * If the object is not available locally or remotely, the client has to tell
 * local scheduler to (re)create the object.
 *
 * This function is non-blocking.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object we want to transfer.
 * @return Status as returned by the get_status() function. Status can take the
 *         following values.
 *         - PLASMA_CLIENT_LOCAL, if the object is stored in the local Plasma
 *           Store.
 *         - PLASMA_CLIENT_TRANSFER, if the object is either currently being
 *           transferred or the transfer has been scheduled.
 *         - PLASMA_CLIENT_REMOTE, if the object is stored at a remote Plasma
 *           Store.
 *         - PLASMA_CLIENT_DOES_NOT_EXIST, if the object doesn’t exist in the
 *           system.
 */
object_status1 plasma_fetch_remote(plasma_connection *conn,
                                   object_id object_id);

/**
 * Return the status of a given object. This function is similar to
 * plasma_fetch_remote() with the only difference that plamsa_fetch_remote()
 * also schedules the obejct transfer, if not local.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose status we query.
 * @return Status as returned by get_status() function. Status can take the
 *         following values.
 *         - PLASMA_CLIENT_LOCAL, if object is stored in the local Plasma Store.
 *           has been already scheduled by the Plasma Manager.
 *         - PLASMA_CLIENT_TRANSFER, if the object is either currently being
 *           transferred or just scheduled.
 *         - PLASMA_CLIENT_REMOTE, if the object is stored at a remote
 *           Plasma Store.
 *         - PLASMA_CLIENT_DOES_NOT_EXIST, if the object doesn’t exist in the
 *           system.
 */
object_status1 plasma_status(plasma_connection *conn, object_id object_id);

/**
 * Return the information associated to a given object.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object whose info the client queries.
 * @param object_info The object's infirmation.
 * @return PLASMA_CLIENT_LOCAL, if the object is in the local Plasma Store.
 *         PLASMA_CLIENT_NOT_LOCAL, if not. In this case, the caller needs to
 *         ignore data, metadata_size, and metadata fields.
 */
int plasma_info(plasma_connection *conn,
                object_id object_id,
                object_info *object_info);

/**
 * Wait for (1) a specified number of objects to be available (sealed) in the
 * local Plasma Store or in a remote Plasma Store, or (2) for a timeout to
 * expire. This is a blocking call.
 *
 * @param conn The object containing the connection state.
 * @param num_object_requests Size of the object_requests array.
 * @param object_requests Object event array. Each element contains a request
 *        for a particular object_id. The type of request is specified in the
 *        "type" field.
 *        - A PLASMA_OBJECT_LOCAL request is satisfied when object_id becomes
 *          available in the local Plasma Store. In this case, this function
 *          sets the "status" field to PLASMA_OBJECT_LOCAL.
 *        - A PLASMA_OBJECT_ANYWHERE request is satisfied when object_id becomes
 *          available either at the local Plasma Store or on a remote Plasma
 *          Store. In this case, the functions sets the "status" field to
 *          PLASMA_OBJECT_LOCAL or PLASMA_OBJECT_REMOTE.
 * @param num_ready_objects The number of requests in object_requests array that
 *        must be satisfied before the function returns, unless it timeouts.
 *        The num_ready_objects should be no larger than num_object_requests.
 * @param timeout_ms  Timeout value in milliseconds. If this timeout expires
 *        before min_num_ready_objects of requests are satisfied, the function
 *        returns.
 * @return Number of satisfied requests in the object_requests list. If the
 *         returned number is less than min_num_ready_objects this means that
 *         timeout expired.
 */
int plasma_wait_for_objects(plasma_connection *conn,
                            int num_object_requests,
                            object_request object_requests[],
                            int num_ready_objects,
                            uint64_t timeout_ms);

/**
 * TODO: maybe move the plasma_client_* functions in another file.
 *
 * plasma_client_* represent functions implemented by client; so probably
 * need to be in a different file.
 */

/**
 * Get an object from the Plasma Store. This function will block until the
 * object has been created and sealed in the Plasma Store.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to get.
 * @param object_buffer The data structure where the object information will be
 *        written, including object payload and metadata.
 * @return Void.
 */
void plasma_client_get(plasma_connection *conn,
                       object_id object_id,
                       object_buffer *object_buffer);

/**
 * Wait for objects to be created (right now, wait for local objects).
 *
 * @param conn The object containing the connection state.
 * @param num_object_ids Number of object IDs wait is called on.
 * @param object_ids Object IDs wait is called on.
 * @param timeout Wait will time out and return after this number of ms.
 * @param num_returns Number of object IDs wait will return if it doesn't time
 *        out.
 * @param return_object_ids Out parameter for the object IDs returned by wait.
 *        This is an array of size num_returns. If the number of objects that
 *        are ready when we time out, the objects will be stored in the last
 *        slots of the array and the number of objects is returned.
 * @return Number of objects that are actually ready.
 */
int plasma_client_wait(plasma_connection *conn,
                       int num_object_ids,
                       object_id object_ids[],
                       uint64_t timeout,
                       int num_returns,
                       object_id return_object_ids[]);

/**
 * Get an array of objects from the Plasma Store. This function will block until
 * all object in the array have been created and sealed in the Plasma Store.
 *
 * @param conn The object containing the connection state.
 * @param num_object_ids The number of objects in the array to be returned.
 * @param object_ids The array of object IDs to be returned.
 * @param object_buffers The array of data structure where the information of
 *                       the return objects will be stored. The objects appear
 *                       in the same order as their IDs in the object_ids array,
 * @return Void.
 */
void plasma_client_multiget(plasma_connection *conn,
                            int num_object_ids,
                            object_id object_ids[],
                            object_buffer object_buffers[]);

/**
 * TODO: maybe move object_requests_* functions in another file.
 * The object_request data structure is defined in plasma.h since
 * it is used by plasma_request and plasma_reply, but there is no
 * plasma.c file.
 */

/**
 * Copy an array of object requests into another one.
 *
 * @param num_object_requests Number of elements in the object_requests arrays.
 * @param object_requests_dst Destination object_requests array.
 * @param object_requests_dst Source object_requests array.
 * @return None.
 */
void object_requests_copy(int num_object_requests,
                          object_request object_requests_dst[],
                          object_request object_requests_src[]);

/**
 * Given an object ID, get the corresponding object request
 * form an array of object requests.
 *
 * @param object_id Identifier of the requested object.
 * @param num_object_requests Number of elements in the object requests array.
 * @param object_requests The array of object requests which
 *        contains the object (object_id).
 * @return Object request, if found; NULL, if not found.
 */
object_request *object_requests_get_object(object_id object_id,
                                           int num_object_requests,
                                           object_request object_requests[]);

/**
 * Initialize status of all object requests in an array.
 *
 * @param num_object_requests Number of elements in the array of object
 *        requests.
 * @param object_requests Array of object requests.
 * @param status Value with which we initialize the status of each object
 *        request in the array.
 * @return Void.
 */
void object_requests_set_status_all(int num_object_requests,
                                    object_request object_requests[],
                                    object_status1 status);

/**
 * Print an object ID with bytes separated by ".".
 *
 * @param object_id Object ID to be printed.
 * @return Void.
 */
void object_id_print(object_id object_id);

/**
 * Print all object requests in an array (for debugging purposes).
 *
 * @param num_object_requests Number of elements in the array of object
 *        requests.
 * @param object_requests Array of object requests.
 * @return Void.
 */
void object_requests_print(int num_object_requests,
                           object_request object_requests[]);

#endif /* PLASMA_CLIENT_H */
