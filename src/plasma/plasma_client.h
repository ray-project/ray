#ifndef PLASMA_CLIENT_H
#define PLASMA_CLIENT_H

#include <stdbool.h>
#include <time.h>

#include "plasma.h"

#define PLASMA_DEFAULT_RELEASE_DELAY 64
/* Use 100MB as an overestimate of the L3 cache size. */
constexpr int64_t kL3CacheSizeBytes = 100000000;

typedef struct PlasmaConnection PlasmaConnection;

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
Status PlasmaConnect(const char *store_socket_name,
                     const char *manager_socket_name,
                     int release_delay, PlasmaConnection **connection);

/**
 * Disconnect from the local plasma instance, including the local store and
 * manager.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return Void.
 */
void plasma_disconnect(PlasmaConnection *conn);

/**
 * Return true if the plasma manager is connected.
 *
 * @param conn The connection to the local plasma store and plasma manager.
 * @return True if the plasma manager is connected and false otherwise.
 */
bool plasma_manager_is_connected(PlasmaConnection *conn);

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
 * @return One of the following error codes:
 *         - PlasmaError_OK, if the object was created successfully.
 *         - PlasmaError_ObjectExists, if an object with this ID is already
 *           present in the store. In this case, the client should not call
 *           plasma_release.
 *         - PlasmaError_OutOfMemory, if the store is out of memory and cannot
 *           create the object. In this case, the client should not call
 *           plasma_release.
 */
 Status PlasmaCreate(PlasmaConnection *conn,
                     ObjectID object_id,
                     int64_t size,
                     uint8_t *metadata,
                     int64_t metadata_size,
                     uint8_t **data);

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
} ObjectBuffer;

/**
 * Get some objects from the Plasma Store. This function will block until the
 * objects have all been created and sealed in the Plasma Store or the timeout
 * expires. The caller is responsible for releasing any retrieved objects, but
 * the caller should not release objects that were not retrieved.
 *
 * @param conn The object containing the connection state.
 * @param object_ids The IDs of the objects to get.
 * @param num_object_ids The number of object IDs to get.
 * @param timeout_ms The amount of time in milliseconds to wait before this
 *        request times out. If this value is -1, then no timeout is set.
 * @param object_buffers An array where the results will be stored. If the data
 *        size field is -1, then the object was not retrieved.
 * @return Void.
 */
Status PlasmaGet(PlasmaConnection *conn,
                ObjectID object_ids[],
                int64_t num_objects,
                int64_t timeout_ms,
                ObjectBuffer object_buffers[]);

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
Status PlasmaRelease(PlasmaConnection *conn, ObjectID object_id);

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
Status PlasmaContains(PlasmaConnection *conn,
                     ObjectID object_id,
                     int *has_object);

/**
 * Compute the hash of an object in the object store.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object we want to hash.
 * @param digest A pointer at which to return the hash digest of the object.
 *        The pointer must have at least DIGEST_SIZE bytes allocated.
 * @return A boolean representing whether the hash operation succeeded.
 */
bool plasma_compute_object_hash(PlasmaConnection *conn,
                                ObjectID object_id,
                                unsigned char *digest);

/**
 * Seal an object in the object store. The object will be immutable after this
 * call.
 *
 * @param conn The object containing the connection state.
 * @param object_id The ID of the object to seal.
 * @return Void.
 */
Status PlasmaSeal(PlasmaConnection *conn, ObjectID object_id);

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
Status PlasmaDelete(PlasmaConnection *conn, ObjectID object_id);

/**
 * Delete objects until we have freed up num_bytes bytes or there are no more
 * released objects that can be deleted.
 *
 * @param conn The object containing the connection state.
 * @param num_bytes The number of bytes to try to free up.
 * @return The total number of bytes of space retrieved.
 */
Status PlasmaEvict(PlasmaConnection *conn, int64_t num_bytes, int64_t &num_bytes_evicted);

/**
 * Attempt to initiate the transfer of some objects from remote Plasma Stores.
 * This method does not guarantee that the fetched objects will arrive locally.
 *
 * For an object that is available in the local Plasma Store, this method will
 * not do anything. For an object that is not available locally, it will check
 * if the object are already being fetched. If so, it will not do anything. If
 * not, it will query the object table for a list of Plasma Managers that have
 * the object. The object table will return a non-empty list, and this Plasma
 * Manager will attempt to initiate transfers from one of those Plasma Managers.
 *
 * This function is non-blocking.
 *
 * This method is idempotent in the sense that it is ok to call it multiple
 * times.
 *
 * @param conn The object containing the connection state.
 * @param num_object_ids The number of object IDs fetch is being called on.
 * @param object_ids The IDs of the objects that fetch is being called on.
 * @return Void.
 */
Status PlasmaFetch(PlasmaConnection *conn,
                  int num_object_ids,
                  ObjectID object_ids[]);

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
Status PlasmaTransfer(PlasmaConnection *conn,
                     const char *addr,
                     int port,
                     ObjectID object_id);

/**
 * Subscribe to notifications when objects are sealed in the object store.
 * Whenever an object is sealed, a message will be written to the client socket
 * that is returned by this method.
 *
 * @param conn The object containing the connection state.
 * @return The file descriptor that the client should use to read notifications
           from the object store about sealed objects.
 */
Status PlasmaSubscribe(PlasmaConnection *conn, int &fd);

/**
 * Get the file descriptor for the socket connection to the plasma manager.
 *
 * @param conn The plasma connection.
 * @return The file descriptor for the manager connection. If there is no
 *         connection to the manager, this is -1.
 */
int get_manager_fd(PlasmaConnection *conn);

/**
 * Return the status of a given object. This method may query the object table.
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
Status PlasmaStatus(PlasmaConnection *conn, ObjectID object_id, int *object_status);

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
int plasma_info(PlasmaConnection *conn,
                ObjectID object_id,
                ObjectInfo *object_info);

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
 *        - A PLASMA_QUERY_LOCAL request is satisfied when object_id becomes
 *          available in the local Plasma Store. In this case, this function
 *          sets the "status" field to ObjectStatus_Local. Note, if the status
 *          is not ObjectStatus_Local, it will be ObjectStatus_Nonexistent,
 *          but it may exist elsewhere in the system.
 *        - A PLASMA_QUERY_ANYWHERE request is satisfied when object_id becomes
 *          available either at the local Plasma Store or on a remote Plasma
 *          Store. In this case, the functions sets the "status" field to
 *          ObjectStatus_Local or ObjectStatus_Remote.
 * @param num_ready_objects The number of requests in object_requests array that
 *        must be satisfied before the function returns, unless it timeouts.
 *        The num_ready_objects should be no larger than num_object_requests.
 * @param timeout_ms Timeout value in milliseconds. If this timeout expires
 *        before min_num_ready_objects of requests are satisfied, the function
 *        returns.
 * @return Number of satisfied requests in the object_requests list. If the
 *         returned number is less than min_num_ready_objects this means that
 *         timeout expired.
 */
Status PlasmaWait(PlasmaConnection *conn,
                  int num_object_requests,
                  ObjectRequest object_requests[],
                  int num_ready_objects,
                  uint64_t timeout_ms,
                  int &num_objects_ready);

#endif /* PLASMA_CLIENT_H */
