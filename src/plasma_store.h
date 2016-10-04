#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include "plasma.h"

/**
 * Create a new object:
 *
 * @param object_id Object ID of the object to be created.
 * @param data_size Size in bytes of the object to be created.
 * @param metadata_size Size in bytes of the object metadata.
 */
void create_object(int conn,
                   object_id object_id,
                   int64_t data_size,
                   int64_t metadata_size,
                   plasma_object *result);

/**
 * Get an object:
 *
 * @param object_id Object ID of the object to be gotten.
 *
 * Returns the status of the object (object_status in plasma.h).
 */
int get_object(int conn, object_id object_id, plasma_object *result);

/**
 * Seal an object:
 *
 * @param object_id Object ID of the object to be sealed.
 * @param conns Returns the connection that are waiting for this object.
                The caller is responsible for destroying this array.
 *
 * Should notify all the sockets waiting for the object.
 */
plasma_object seal_object(int conn,
                          object_id object_id,
                          UT_array **conns,
                          plasma_object *result);

/**
 * Check if the plasma store contains an object:
 *
 * @param object_id Object ID that will be checked.
 */
int contains_object(int conn, object_id object_id);

#endif /* PLASMA_STORE_H */
