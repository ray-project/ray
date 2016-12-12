#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#define PLASMA_PROTOCOL_VERSION 0x0000000000000000

#include "common.h"
#include "plasma.h"

#include "format/plasma_reader.h"

/**
 * Send a message of a given type containing just an object id.
 *
 * @param sock Socket on which the message is sent.
 * @param message_type Message type.
 * @param object_id ID of the object to be sent
 * @return Propagate finalize_buffer_and_send()return value.
 */
int send_object_id(int sock, int message_type, object_id object_id);

/**
 * Read the payload of a message which contains only object id.
 *
 * @param data Message payload.
 * @param object_id ID of the object to be read from the payload.
 * @return Void.
 */
void read_object_id(uint8_t *data, object_id *object_id);

/**
 * Send a message of a given type containing an object id and
 * its associated information.
 *
 * @param sock Socket on which the message is sent.
 * @param message_type Message type.
 * @param object_id ID of the object to be sent
 * @param object_info Information associated with the object (e.g, error, status)
 * @return Propagate finalize_buffer_and_send()return value.
 */
int send_object_id_and_info(int sock, int message_type, object_id object_id, int object_info);

/**
 * Read the payload of a message which contains and object id,
 * and its associated information.
 *
 * @param data Message payload.
 * @param object_id ID of the object to be read from the payload.
 * @param object_info Information of the object to be read from the payload.
 * @return Void.
 */
void read_object_id_and_info(uint8_t *data, object_id *object_id, int *object_info);

/**
 * Send a message of a given type containing an array of object ids.
 *
 * @param sock Socket on which the message is sent.
 * @param message_type Message type.
 * @param object_ids Array of object IDs to be sent
 * @param num_objects Number of object IDs to be sent.
 * @return Propagate finalize_buffer_and_send()return value.
 */
int send_object_ids(int sock,
                    int message_type,
                    object_id object_ids[],
                    int64_t num_objects);

/**
 * Read the payload of a message which contains an array of object ids.
 *
 * @param data Message payload.
 * @param object_ids_ptr Array of pointers of object IDs are written.
 *                       This array is allocated by the function so it must be feed by the caller.
 *                       The reason of why this is allocated by this function instead of the caller
 *                       is because the caller doesn't know the size of the array.
 * @param num_objects Number of objects in the array.
 *
 * @return Void.
 */
void read_object_ids(uint8_t *data,
                     object_id** object_ids_ptr,
                     int64_t *num_objects);

/**
 * Send a message of a given type containing an array of object ids
 * and their information (e.g., status).
 *
 * @param sock Socket on which the message is sent.
 * @param message_type Message type.
 * @param object_ids Array of object IDs to be sent
 * @param object_infos Array of info associated to each object ID.
 * @param num_objects Number of object IDs to be sent.
 * @return Propagate finalize_buffer_and_send()return value.
 */
int send_object_ids_and_infos(int sock,
                              int message_type,
                              object_id object_ids[],
                              int32_t object_infos[],
                              int64_t num_objects);

/**
 * Read the payload of a message which contains an array of object ids and their
 * associated information (e.g., status).
 *
 * @param data Message payload.
 * @param object_ids_ptr Array of pointers of object IDs are written.
 *                       This array is allocated by the function so it must be free by the caller.
 * @param object_infos Array of informations associated with the objects whose IDs is returned.
 * @param num_objects Number of objects in the array.
 * @return Void.
 */
void read_object_ids_and_infos(uint8_t *data,
                               object_id** object_ids_ptr,
                               int32_t object_infos[],
                               int64_t *num_objects);

/**
 * Plasma Create message functions.
 */
int plasma_send_create_request(int sock,
                               object_id object_id,
                               int64_t data_size,
                               int64_t metadata_size);

void plasma_read_create_request(uint8_t *data,
                                object_id *object_id,
                                int64_t *data_size,
                                int64_t *metadata_size);

int plasma_send_create_reply(int sock,
                             object_id object_id,
                             plasma_object *object,
                             int error_code);

void plasma_read_create_reply(uint8_t *data,
                              object_id *object_id,
                              plasma_object *object,
                              int *error_code);

/**
 * Plasma Seal message functions.
 */
#define plasma_send_seal_request(sock, object_id) \
  send_object_id(sock, MessageType_PlasmaSealRequest, object_id)

#define plasma_read_seal_request(data, object_id) \
  read_object_id(data, object_id)

#define plasma_send_seal_reply(sock, object_id, error) \
  send_object_id_and_info(sock, MessageType_PlasmaSealReply, object_id, error)

#define plasma_read_seal_reply(data, object_id, error) \
  read_object_id_and_info(data, object_id, error)

/**
 * Plasma Release message functions.
 */
#define plasma_send_release_request(sock, object_id) \
  send_object_id(sock, MessageType_PlasmaReleaseRequest, object_id)

#define plasma_read_release_request(data, object_id) \
  read_object_id(data, object_id)

#define plasma_send_release_reply(sock, object_id, error) \
  send_object_id_and_info(sock, MessageType_PlasmaReleaseReply, object_id, error)

#define plasma_read_release_reply(data, object_id, error) \
  read_object_id_and_info(data, object_id, error)

/**
 * Plasma Delete message functions.
 */
#define plasma_send_delete_request(sock, object_id) \
  send_object_id(sock, MessageType_PlasmaDeleteRequest, object_id)

#define plasma_read_delete_request(data, object_id) \
  read_object_id(data, object_id)

#define plasma_send_delete_reply(sock, object_id, error) \
  send_object_id_and_info(sock, MessageType_PlasmaDeleteReply, object_id, error)

#define plasma_read_delete_reply(data, object_id, error) \
  read_object_id_and_info(data, object_id, error)


/**
 * Plasma Get Local message functions.
 */

#define plasma_send_get_local_request(sock, object_ids, num_objects) \
  send_object_ids(sock, MessageType_PlasmaGetLocalRequest, object_ids, num_objects)

#define plasma_read_get_local_request(data, object_ids, num_objects) \
  read_object_ids(data, object_ids, num_objects)

int plasma_send_get_local_reply(int sock,
                                object_id object_ids[],
                                plasma_object plasma_objects[],
                                int64_t num_objects);

void plasma_read_get_local_reply(uint8_t *data,
                                 object_id **object_ids_ptr,
                                 plasma_object plasma_objects[],
                                 int64_t *num_objects);

/**
 * Plasma Evict message functions (no reply so far...).
 */

#define plasma_send_evict_request(sock, object_ids, num_objects) \
  send_object_ids(sock, MessageType_PlasmaEvictRequest, object_ids, num_objects)

#define plasma_read_evict_request(data, object_ids_ptr, num_objects) \
  read_object_ids(data, object_ids_ptr, num_objects)

/**
 * Plasma Fetch Remote message functions.
 */

#define plasma_send_fetch_remote_request(sock, object_ids, num_objects) \
  send_object_ids(sock, MessageType_PlasmaFetchRemoteRequest, object_ids, num_objects)

#define plasma_read_fetch_remote_request(data, object_ids_ptr, num_objects) \
  read_object_ids(data, object_ids_ptr, num_objects)

#define plasma_send_fetch_remote_reply(sock, object_ids, object_infos, num_objects) \
  send_object_ids_and_infos(sock, MessageType_PlasmaFetchRemoteReply, object_ids, object_infos, num_objects)

#define plasma_read_fetch_remote_reply(data, object_ids_ptr, object_infos, num_objects) \
  read_object_ids_and_infos(data, object_ids_ptr, object_infos, num_objects)

/**
 * Plasma Fetch Remote No Reply message functions.
 */

#define plasma_send_fetch_remote_no_reply_request(sock, object_ids, num_objects) \
  send_object_ids(sock, MessageType_PlasmaFetchRemoteNoReplyRequest, object_ids, num_objects)

#define plasma_read_fetch_remote_no_reply_request(data, object_ids_ptr, num_objects) \
  read_object_ids(data, object_ids_ptr, num_objects)

/**
 * Plasma Wait message functions.
 */
int plasma_send_wait_request(int sock,
                             object_request object_requests[],
                             int num_requests,
                             int num_ready_objects,
                             int64_t timeout_ms);

void plasma_read_wait_request(uint8_t *data,
                              object_request object_requests[],
                              int *num_ready_objects);


int send_data_int(int sock, int message_type, int value);

void read_data_int(uint8_t *data, int *value);

#endif /* PLASMA_PROTOCOL */
