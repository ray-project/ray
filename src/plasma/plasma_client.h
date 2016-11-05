#ifndef PLASMA_CLIENT_H
#define PLASMA_CLIENT_H

#include "plasma.h"

typedef struct plasma_connection plasma_connection;

/**
 * This is used by the Plasma Client to send a request to the Plasma Store or
 * the Plasma Manager.
 *
 * @param conn The file descriptor to use to send the request.
 * @param type The type of request.
 * @param req The address of the request to send.
 * @return Void.
 */
void plasma_send_request(int fd, int type, plasma_request *req);

/**
 * Create a plasma request to be sent with a single object ID.
 *
 * @param object_id The object ID to include in the request.
 * @return The plasma request.
 */
plasma_request make_plasma_request(object_id object_id);

/**
 * Create a plasma request to be sent with multiple object ID. Caller must free
 * the returned plasma request pointer.
 *
 * @param num_object_ids The number of object IDs to include in the request.
 * @param object_ids The array of object IDs to include in the request. It must
 *        have length at least equal to num_object_ids.
 * @return A pointer to the newly created plasma request.
 */
plasma_request *make_plasma_multiple_request(int num_object_ids,
                                             object_id object_ids[]);

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
                                  const char *manager_socket_name);

/**
 * Disconnect from the local plasma instance, including the local store and
 * manager.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return Void.
 */
void plasma_disconnect(plasma_connection *conn);

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
 * @return Void.
 */
void plasma_create(plasma_connection *conn,
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

// TODO: document
void plasma_init_kvstore(plasma_connection *conn,
                         object_id kv_object_id,
                         void **shards,
                         uint64_t *shard_sizes,
                         uint64_t num_shards,
                         char shard_order,
                         uint64_t *shape,
                         uint64_t ndims);

// TODO: document
typedef struct {
  void **shards_handle;
  uint64_t num_shards;
  uint64_t *shape;
  uint64_t ndim;
  uint64_t *shard_sizes;
  uint64_t start_axis_idx;
  char shard_order;
} plasma_pull_result;

// TODO: document
void plasma_pull(plasma_connection *conn,
                 object_id kv_object_id,
                 uint64_t range_start,
                 uint64_t range_end,
                 plasma_pull_result *result);

// TODO: document
void plasma_push(plasma_connection *conn,
                 object_id kv_object_id,
                 uint64_t range_start,
                 uint64_t range_end,
                 uint64_t size,
                 void* data);

#endif
